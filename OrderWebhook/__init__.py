import logging
import azure.functions as func
import os
import traceback
import json
from datetime import datetime

# Configure logging to ensure it captures properly in Azure
logger = logging.getLogger(__name__)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # --- 1. CONFIGURATION GROUPING ---
        # We read all environment variables at the start of the function
        # This makes it easy to see what the function depends on
        BLOB_CONN_STR = os.environ.get("BLOB_CONN_STR")
        BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")
        EXCEL_BLOB_NAME = os.environ.get("EXCEL_BLOB_NAME")

        OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
        OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
        WHISPER_DEPLOY = os.environ.get("AZURE_OPENAI_WHISPER_DEPLOYMENT")
        GPT_DEPLOY = os.environ.get("AZURE_OPENAI_GPT_DEPLOYMENT")

        TWILIO_SID = os.environ.get("TWILIO_SID")
        TWILIO_AUTH = os.environ.get("TWILIO_AUTH")
        TWILIO_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER")

        # Imports grouped for speed and error isolation
        from urllib.parse import parse_qs
        import uuid
        import tempfile

        logging.info("--- Processing New WhatsApp Request ---")

        # --- 2. VALIDATION ---
        if not all([BLOB_CONN_STR, OPENAI_KEY, TWILIO_SID]):
            logging.error("Missing critical environment variables. Check Azure App Settings.")
            return func.HttpResponse("Server configuration error", status_code=500)

        # --- 3. REQUEST PARSING ---
        content_type = (req.headers.get("Content-Type") or "").lower()
        if "application/x-www-form-urlencoded" in content_type:
            body = req.get_body().decode("utf-8")
            form = {k: v[0] for k, v in parse_qs(body).items()}
        else:
            form = req.get_json() if req.get_body() else {}

        media_url = form.get("MediaUrl0") or form.get("MediaUrl")
        from_number = form.get("From", "").replace("whatsapp:", "")

        if not media_url:
            logging.warning(f"Request from {from_number} ignored: No MediaUrl found.")
            return func.HttpResponse("Accepted", status_code=200)

        # --- 4. CORE LOGIC ---
        logging.info(f"Downloading audio for customer: {from_number}")
        voice_path = download_raw_voice(media_url, sid=TWILIO_SID, auth=TWILIO_AUTH)
        
        try:
            # Transcription (Whisper)
            logging.info("Sending audio to Azure OpenAI Whisper...")
            transcript = transcribe_whisper(voice_path, OPENAI_ENDPOINT, OPENAI_KEY, WHISPER_DEPLOY)
            logging.info(f"Transcription result: {transcript}")

            if not transcript.strip():
                logging.error("Whisper returned empty text.")
                return func.HttpResponse("Could not understand audio", status_code=200)

            # Extraction (GPT)
            logging.info("Extracting order details using GPT...")
            order_data = extract_order_json(transcript, endpoint=OPENAI_ENDPOINT, key=OPENAI_KEY, deployment=GPT_DEPLOY)
            logging.info(f"Structured Order: {json.dumps(order_data)}")

            # Excel Logging
            logging.info(f"Updating Excel sheet: {EXCEL_BLOB_NAME}")
            log_to_excel(order_data, from_number, conn=BLOB_CONN_STR, container=BLOB_CONTAINER, blob=EXCEL_BLOB_NAME)

            # Customer Response
            logging.info(f"Sending WhatsApp invoice to {from_number}")
            invoice_msg = format_invoice(order_data)
            send_whatsapp_message(from_number, invoice_msg, TWILIO_SID, TWILIO_AUTH, TWILIO_NUMBER)
            
            logging.info("--- Request Successfully Processed ---")
            return func.HttpResponse(json.dumps({"status": "success"}), mimetype="application/json")

        finally:
            # Always clean up the temp file even if the code crashes
            if os.path.exists(voice_path):
                os.remove(voice_path)
                logging.info("Temporary audio file deleted.")

    except Exception:
        # Log the full error for debugging in Application Insights
        logging.error(f"CRITICAL ERROR: {traceback.format_exc()}")
        return func.HttpResponse("Internal processing error", status_code=500)

# ---------------- HELPER FUNCTIONS (Encapsulated) ----------------

def download_raw_voice(url, sid, auth):
    import requests
    import tempfile
    import uuid
    r = requests.get(url, auth=(sid, auth), stream=True, timeout=30)
    r.raise_for_status()
    path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4().hex}.ogg")
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    return path

def transcribe_whisper(file_path, endpoint, key, deployment):
    from openai import AzureOpenAI
    client = AzureOpenAI(api_key=key, api_version="2024-06-01", azure_endpoint=endpoint)
    with open(file_path, "rb") as audio:
        result = client.audio.transcriptions.create(model=deployment, file=audio)
    return result.text

def extract_order_json(text, endpoint, key, deployment):
    from openai import AzureOpenAI
    client = AzureOpenAI(api_key=key, api_version="2024-02-15-preview", azure_endpoint=endpoint)
    
    # Precise prompt to ensure GPT doesn't add conversational filler
    prompt = (
        f"Extract order from: '{text}'. "
        "Return ONLY valid JSON with structure: "
        "{'items': [{'name':str, 'qty':int, 'price':int}], 'total':int, 'currency':str}"
    )
    
    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": "You are a JSON-only order processing bot."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    # Parse the text response into a Python dictionary
    return json.loads(response.choices[0].message.content)

def format_invoice(data):
    """Formats a user-friendly WhatsApp message with Markdown bolding."""
    msg = "✅ *Order Confirmed*\n---\n"
    for item in data.get('items', []):
        name = item.get('name', 'Item')
        qty = item.get('qty', 1)
        price = item.get('price', 0)
        msg += f"• {name} (x{qty}): {qty * price} {data.get('currency', '')}\n"
    msg += f"\n*Total Amount: {data.get('total', 0)} {data.get('currency', '')}*"
    msg += "\n\nWe are preparing your order now!"
    return msg

def send_whatsapp_message(to, body, sid, auth, from_num):
    from twilio.rest import Client

    # 1. Clean and prefix the 'from' number
    sender = from_num.strip()
    if not sender.startswith("whatsapp:"):
        sender = f"whatsapp:{sender}"
    
    # 2. Clean and prefix the 'to' number
    recipient = to.strip()
    if not recipient.startswith("whatsapp:"):
        recipient = f"whatsapp:{recipient}"

    # --- LOGGING THE ATTEMPT ---
    logging.info("--- TWILIO OUTBOUND LOG ---")
    logging.info(f"SENDER:    [{sender}]")
    logging.info(f"RECIPIENT: [{recipient}]")
    logging.info(f"MESSAGE:   {body[:50]}...") # Logs first 50 chars of the message
    
    client = Client(sid, auth)
    
    try:
        message = client.messages.create(
            body=body,
            from_=sender,
            to=recipient
        )
        logging.info(f"SUCCESS: Message SID {message.sid}")
        return message.sid
    except Exception as e:
        # Logs the specific error from Twilio
        logging.error(f"TWILIO FAILURE: {str(e)}")
        raise e

def log_to_excel(data, customer, conn, container, blob):
    from azure.storage.blob import BlobServiceClient
    from openpyxl import load_workbook, Workbook
    import tempfile
    
    service = BlobServiceClient.from_connection_string(conn)
    b_client = service.get_blob_client(container, blob)
    tmp = os.path.join(tempfile.gettempdir(), f"sync_{customer}.xlsx")
    
    # Download existing or create new
    try:
        with open(tmp, "wb") as f: 
            f.write(b_client.download_blob().readall())
        wb = load_workbook(tmp)
    except Exception:
        wb = Workbook()
        wb.active.append(["Date", "Customer", "Items", "Total"])
    
    ws = wb.active
    # Create a string summary of items for the Excel cell
    summary = ", ".join([f"{i['name']} x{i['qty']}" for i in data.get('items', [])])
    
    ws.append([
        datetime.utcnow().strftime("%Y-%m-%d %H:%M"), 
        customer, 
        summary, 
        data.get('total')
    ])
    
    wb.save(tmp)
    with open(tmp, "rb") as f: 
        b_client.upload_blob(f, overwrite=True)