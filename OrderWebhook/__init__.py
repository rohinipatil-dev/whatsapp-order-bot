import logging
import azure.functions as func
import requests
import os
from datetime import datetime
from openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("WhatsApp voice webhook triggered.")

        # Environment variables (move here to catch errors)
        BLOB_CONN_STR = os.environ.get("BLOB_CONN_STR")
        BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")
        EXCEL_BLOB_NAME = os.environ.get("EXCEL_BLOB_NAME")
        AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
        AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")

        if not all([BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY]):
            return func.HttpResponse("Missing environment variables", status_code=500)

        # Step 1: Parse Twilio webhook payload
        media_url = req.form.get("MediaUrl0")
        from_number = req.form.get("From")

        if not media_url:
            return func.HttpResponse("No media found", status_code=400)

        # Step 2: Download voice file
        voice_file_path = download_voice(media_url)

        # Step 3: Upload to Azure Blob
        blob_url = upload_to_blob(voice_file_path)

        # Step 4: Transcribe using Azure Speech (or Foundry)
        transcription = transcribe_audio(blob_url)

        # Step 5: Parse item & quantity using Azure OpenAI
        parsed_order = parse_order(transcription)

        # Step 6: Log to Excel
        log_to_excel(parsed_order, from_number)

        # Step 7: Optional: send confirmation (via Twilio API)
        # send_confirmation(from_number, parsed_order)

        return func.HttpResponse(f"Order logged: {parsed_order}")
    except Exception as e:
        logging.exception("Unhandled error in WhatsApp webhook")
        return func.HttpResponse(f"Internal server error: {e}", status_code=500)


# ---------------- Helper functions ----------------
def download_voice(media_url):
    r = requests.get(media_url)
    filename = "temp_voice.ogg"
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename

def upload_to_blob(local_file):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER, blob=local_file)
    with open(local_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    return blob_client.url

def transcribe_audio(blob_url):
    # Placeholder for Azure Speech or Foundry transcription
    # Example using Azure Speech SDK:
    # from azure.cognitiveservices.speech import SpeechConfig, AudioConfig, SpeechRecognizer
    return "I want 3 bottles of water"  # Example transcription

def parse_order(transcribed_text):
    # Placeholder using Azure OpenAI (Foundry agent can also be used)
    # Prompt example: extract item & quantity
    # For simplicity, using regex demo here
    import re
    match = re.search(r"(\d+)\s+(\w+)", transcribed_text)
    if match:
        return {"quantity": int(match.group(1)), "item": match.group(2)}
    else:
        return {"quantity": None, "item": None}

def log_to_excel(order, customer_number):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER, blob=EXCEL_BLOB_NAME)

    # Download existing file
    download_file = "temp_orders.xlsx"
    with open(download_file, "wb") as f:
        f.write(blob_client.download_blob().readall())

    wb = load_workbook(download_file)
    ws = wb.active
    ws.append([datetime.utcnow(), customer_number, order["item"], order["quantity"]])
    wb.save(download_file)

    # Upload updated file
    with open(download_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

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
