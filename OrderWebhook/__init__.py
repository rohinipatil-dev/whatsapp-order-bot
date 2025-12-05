import logging
import azure.functions as func
import requests
import os
from datetime import datetime
from openpyxl import load_workbook, Workbook
from azure.storage.blob import BlobServiceClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from urllib.parse import parse_qs
import uuid
import tempfile
import traceback

# NOTE: this module reads environment variables inside main and passes them into helper
# functions to avoid NameError / scope issues that caused the 500 you observed.

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("WhatsApp voice webhook triggered.")

        # Read environment variables (fail early with clear message)
        BLOB_CONN_STR = os.environ.get("BLOB_CONN_STR")
        BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")
        EXCEL_BLOB_NAME = os.environ.get("EXCEL_BLOB_NAME")
        AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
        AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")

        if not all([BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY]):
            logging.error("Missing one or more required environment variables")
            return func.HttpResponse("Missing environment variables", status_code=500)

        # Parse incoming form-encoded Twilio payload robustly
        content_type = req.headers.get("Content-Type", "")
        form = {}
        if "application/x-www-form-urlencoded" in content_type:
            body = req.get_body().decode("utf-8", errors="replace")
            parsed = parse_qs(body)
            # parse_qs gives lists for each key
            form = {k: v[0] if isinstance(v, list) and len(v) > 0 else "" for k, v in parsed.items()}
        else:
            # Try to read req.form if available or fallback to json
            try:
                # azure.functions.HttpRequest may populate .form when content-type is form data
                if hasattr(req, "form"):
                    form = {k: v for k, v in req.form.items()}
                else:
                    form = req.get_json() if req.get_body() else {}
            except Exception:
                # last fallback: parse empty
                form = {}

        media_url = form.get("MediaUrl0") or form.get("MediaUrl") or ""
        from_number = form.get("From") or ""

        if not media_url:
            logging.warning("No media URL found in request")
            return func.HttpResponse("No media found", status_code=400)

        # Step 2: Download voice file (to a temp file)
        voice_file_path = download_voice(media_url)

        try:
            # Step 3: Upload to Azure Blob
            blob_url = upload_to_blob(voice_file_path, BLOB_CONN_STR, BLOB_CONTAINER)

            # Step 4: Transcribe using Azure Speech (or Foundry)
            transcription = transcribe_audio(blob_url)

            # Step 5: Parse item & quantity using Azure OpenAI
            parsed_order = parse_order(transcription)

            # Step 6: Log to Excel
            log_to_excel(parsed_order, from_number, BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME)

            # Step 7: Optional: send confirmation (via Twilio API)
            # send_confirmation(from_number, parsed_order)

            logging.info("Order processed successfully: %s", parsed_order)
            return func.HttpResponse(f"Order logged: {parsed_order}", status_code=200)
        finally:
            # Cleanup temp downloaded file
            try:
                if voice_file_path and os.path.exists(voice_file_path):
                    os.remove(voice_file_path)
            except Exception:
                logging.exception("Failed to remove temp voice file")

    except Exception as e:
        logging.exception("Unhandled error in WhatsApp webhook")
        tb = traceback.format_exc()
        # Put full traceback in logs (not in response) to avoid leaking secrets
        logging.error("Traceback:\n%s", tb)
        return func.HttpResponse("Internal server error", status_code=500)


# ---------------- Helper functions ----------------
def download_voice(media_url, timeout=15):
    """
    Downloads the media URL to a uniquely named temp file and returns the path.
    Raises on network errors so caller can log and return proper status code.
    """
    logging.info("Downloading media from %s", media_url)
    r = requests.get(media_url, stream=True, timeout=timeout)
    r.raise_for_status()
    suffix = os.path.splitext(media_url.split("?")[0])[1] or ".ogg"
    filename = os.path.join(tempfile.gettempdir(), f"temp_voice_{uuid.uuid4().hex}{suffix}")
    with open(filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    logging.info("Downloaded media to %s", filename)
    return filename


def upload_to_blob(local_file, conn_str, container_name):
    """
    Uploads local_file to the specified container and returns the blob URL.
    Uses a timestamped/uuid blob name to avoid collisions.
    """
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    # Ensure container exists (idempotent)
    try:
        container_client.create_container()
        logging.info("Created container %s", container_name)
    except Exception:
        # Likely already exists; ignore
        pass

    blob_name = f"voices/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}{os.path.splitext(local_file)[1]}"
    blob_client = container_client.get_blob_client(blob=blob_name)
    logging.info("Uploading file %s to blob %s", local_file, blob_name)
    with open(local_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info("Uploaded blob url: %s", blob_client.url)
    return blob_client.url


def transcribe_audio(blob_url):
    """
    Placeholder transcription. Replace with your Azure Speech SDK / Foundry call.
    """
    logging.info("Transcribing audio at %s", blob_url)
    # TODO: implement actual transcription using speech SDK or Foundry API
    # Return a string transcription for downstream parsing
    return "I want 3 bottles of water"


def parse_order(transcribed_text):
    """
    Very simple parser for demo purposes. Replace with an OpenAI/LLM call if needed.
    """
    logging.info("Parsing transcription: %s", transcribed_text)
    import re
    match = re.search(r"(\d+)\s+([A-Za-z]+)", transcribed_text)
    if match:
        return {"quantity": int(match.group(1)), "item": match.group(2)}
    else:
        return {"quantity": None, "item": None}


def log_to_excel(order, customer_number, conn_str, container_name, excel_blob_name):
    """
    Downloads the Excel workbook from blob storage (or creates one if missing),
    appends the order row, and uploads it back.
    """
    logging.info("Logging order to Excel: %s", order)
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(excel_blob_name)

    # Prepare temp file paths
    download_file = os.path.join(tempfile.gettempdir(), f"orders_{uuid.uuid4().hex}.xlsx")
    created_new = False

    try:
        logging.info("Downloading Excel blob %s from container %s", excel_blob_name, container_name)
        with open(download_file, "wb") as f:
            stream = blob_client.download_blob()
            f.write(stream.readall())
    except Exception as ex:
        logging.warning("Could not download existing workbook: %s. Will create a new workbook. Error: %s", excel_blob_name, ex)
        wb = Workbook()
        ws = wb.active
        ws.append(["timestamp_utc", "customer_number", "item", "quantity"])
        created_new = True
    else:
        wb = load_workbook(download_file)
        ws = wb.active

    # Append row and save
    ws.append([datetime.utcnow().isoformat(), customer_number, order.get("item"), order.get("quantity")])
    wb.save(download_file)

    # Upload updated workbook
    with open(download_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info("Uploaded updated Excel to blob %s", excel_blob_name)

    # Cleanup temp file
    try:
        if os.path.exists(download_file):
            os.remove(download_file)
    except Exception:
        logging.exception("Failed to remove temp excel file")


# Optional Twilio confirmation
def send_confirmation(to_number, order):
    from twilio.rest import Client
    TWILIO_SID = os.environ["TWILIO_SID"]
    TWILIO_AUTH = os.environ["TWILIO_AUTH"]
    TWILIO_WHATSAPP = os.environ["TWILIO_WHATSAPP_NUMBER"]

    client = Client(TWILIO_SID, TWILIO_AUTH)
    message = f"Your order of {order['quantity']} {order['item']} has been logged."
    client.messages.create(
        body=message,
        from_=f"whatsapp:{TWILIO_WHATSAPP}",
        to=f"whatsapp:{to_number}"
    )