import logging
import azure.functions as func
import requests
import os
from datetime import datetime
from openpyxl import load_workbook, Workbook
from azure.storage.blob import BlobServiceClient
from urllib.parse import parse_qs, urlparse
import uuid
import tempfile
import traceback
import json
import re

# NOTE: This module expects the following environment variables to be set for full functionality:
# - BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME
# - AZURE_SPEECH_KEY, AZURE_SPEECH_REGION        (for Azure Speech SDK transcription)
# - AZURE_SPEECH_LANGUAGES (optional, comma-separated language tags like "ar-SA,en-US" for auto-detect)
# - AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT (Azure OpenAI deployment name)
# - TWILIO_SID, TWILIO_AUTH (for downloading Twilio-hosted media)
#
# If Speech SDK or OpenAI is not configured, code falls back to safer defaults (empty transcription or regex parse).
# Be careful not to log secret values.

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("WhatsApp voice webhook triggered.")

        # Read required environment variables
        BLOB_CONN_STR = os.environ.get("BLOB_CONN_STR")
        BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER")
        EXCEL_BLOB_NAME = os.environ.get("EXCEL_BLOB_NAME")

        # Azure Speech
        AZURE_SPEECH_KEY = os.environ.get("AZURE_SPEECH_KEY")
        AZURE_SPEECH_REGION = os.environ.get("AZURE_SPEECH_REGION")
        AZURE_SPEECH_LANGUAGES = os.environ.get("AZURE_SPEECH_LANGUAGES")  # comma-separated list for auto-detect

        # Azure OpenAI (these remain optional; parse_order will check)
        AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
        AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
        AZURE_OPENAI_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT")

        # Twilio
        TWILIO_SID = os.environ.get("TWILIO_SID")
        TWILIO_AUTH = os.environ.get("TWILIO_AUTH")

        if not all([BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME]):
            logging.error("Missing one or more required storage environment variables (BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME).")
            return func.HttpResponse("Missing storage configuration", status_code=500)

        # Parse Twilio form or JSON
        content_type = (req.headers.get("Content-Type") or req.headers.get("content-type") or "").lower()
        form = {}
        if "application/x-www-form-urlencoded" in content_type:
            body = req.get_body().decode("utf-8", errors="replace")
            parsed = parse_qs(body)
            # parse_qs gives lists for each key
            form = {k: v[0] if isinstance(v, list) and len(v) > 0 else "" for k, v in parsed.items()}
        else:
            # Try to read req.form if available or fallback to json
            try:
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
        # Pass Twilio credentials so requests can authenticate when downloading Twilio-hosted media
        voice_file_path = download_voice(media_url, twilio_sid=TWILIO_SID, twilio_auth=TWILIO_AUTH)

        try:
            # Step 3: Upload to Azure Blob
            blob_url = upload_to_blob(voice_file_path, BLOB_CONN_STR, BLOB_CONTAINER)

            # Step 4: Transcribe: use Speech SDK if configured
            speech_langs = None
            if AZURE_SPEECH_LANGUAGES:
                speech_langs = [l.strip() for l in AZURE_SPEECH_LANGUAGES.split(",") if l.strip()]
            transcription = transcribe_audio(
                blob_url,
                conn_str=BLOB_CONN_STR,
                speech_key=AZURE_SPEECH_KEY,
                speech_region=AZURE_SPEECH_REGION,
                auto_detect_languages=speech_langs
            )

            # Step 5: Parse using Azure OpenAI (via openai package) if configured else fallback regex
            parsed_order = parse_order(
                transcription,
                openai_endpoint=AZURE_OPENAI_ENDPOINT,
                openai_key=AZURE_OPENAI_KEY,
                openai_deployment=AZURE_OPENAI_DEPLOYMENT
            )

            # Step 6: Log to Excel
            log_to_excel(parsed_order, from_number, BLOB_CONN_STR, BLOB_CONTAINER, EXCEL_BLOB_NAME)

            # Step 7: Optional: send confirmation (via Twilio API)
            # send_confirmation(from_number, parsed_order)

            logging.info("Order processed successfully: %s", parsed_order)
            return func.HttpResponse(json.dumps({"status": "ok", "order": parsed_order, "blob_url": blob_url}), status_code=200, mimetype="application/json")
        finally:
            # Cleanup temp downloaded file
            try:
                if voice_file_path and os.path.exists(voice_file_path):
                    os.remove(voice_file_path)
            except Exception:
                logging.exception("Failed to cleanup temp file")

    except Exception as e:
        logging.exception("Unhandled error in WhatsApp webhook")
        tb = traceback.format_exc()
        # Put full traceback in logs (not in response) to avoid leaking secrets
        logging.error("Traceback:\n%s", tb)
        return func.HttpResponse("Internal server error", status_code=500)


# ---------------- Helper functions ----------------

def download_voice(media_url, twilio_sid=None, twilio_auth=None, timeout=30):
    logging.info("Downloading media from %s", media_url)
    auth = (twilio_sid, twilio_auth) if twilio_sid and twilio_auth else None
    r = requests.get(media_url, stream=True, timeout=timeout, auth=auth)
    r.raise_for_status()
    suffix = os.path.splitext(urlparse(media_url).path)[1] or ".ogg"
    filename = os.path.join(tempfile.gettempdir(), f"temp_voice_{uuid.uuid4().hex}{suffix}")
    with open(filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    logging.info("Downloaded media to %s", filename)
    return filename


def upload_to_blob(local_file, conn_str, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
        logging.info("Created container %s", container_name)
    except Exception:
        pass
    blob_name = f"voices/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}{os.path.splitext(local_file)[1]}"
    blob_client = container_client.get_blob_client(blob=blob_name)
    logging.info("Uploading file %s to blob %s", local_file, blob_name)
    with open(local_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info("Uploaded blob url: %s", blob_client.url)
    return blob_client.url


def transcribe_audio(blob_url, conn_str=None, speech_key=None, speech_region=None, auto_detect_languages=None):
    logging.info("Transcribing audio at %s", blob_url)
    download_path = None
    try:
        if conn_str:
            parsed = urlparse(blob_url)
            path = parsed.path.lstrip("/")
            parts = path.split("/", 1)
            if len(parts) != 2:
                raise ValueError("Unable to parse container/blob from blob_url")
            container_name, blob_name = parts[0], parts[1]
            blob_client = BlobServiceClient.from_connection_string(conn_str).get_blob_client(container=container_name, blob=blob_name)
            download_path = os.path.join(tempfile.gettempdir(), f"speech_{uuid.uuid4().hex}{os.path.splitext(blob_name)[1] or '.wav'}")
            with open(download_path, "wb") as f:
                stream = blob_client.download_blob()
                f.write(stream.readall())
        else:
            r = requests.get(blob_url, stream=True, timeout=60)
            r.raise_for_status()
            suffix = os.path.splitext(urlparse(blob_url).path)[1] or ".ogg"
            download_path = os.path.join(tempfile.gettempdir(), f"speech_{uuid.uuid4().hex}{suffix}")
            with open(download_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        # Use Speech SDK if possible
        if speech_key and speech_region:
            try:
                import azure.cognitiveservices.speech as speechsdk
            except Exception as e:
                logging.exception("Speech SDK import failed. Ensure azure-cognitiveservices-speech is installed. Error: %s", e)
                return ""

            speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region)
            audio_config = speechsdk.AudioConfig(filename=download_path)

            if auto_detect_languages and isinstance(auto_detect_languages, (list, tuple)) and len(auto_detect_languages) > 0:
                try:
                    auto_detect_config = speechsdk.AutoDetectSourceLanguageConfig(languages=auto_detect_languages)
                    recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config, auto_detect_source_language_config=auto_detect_config)
                    result = recognizer.recognize_once()
                    try:
                        detection = result.properties.get(speechsdk.PropertyId.SpeechServiceConnection_AutoDetectSourceLanguageResult)
                        logging.info("Auto-detected language result: %s", detection)
                    except Exception:
                        pass
                except Exception:
                    logging.exception("Auto-detect language failed; falling back to single-language recognition")
                    recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)
                    result = recognizer.recognize_once()
            else:
                recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)
                result = recognizer.recognize_once()

            if result.reason == speechsdk.ResultReason.RecognizedSpeech:
                logging.info("Transcription success (length %d): %s", len(result.text or ""), (result.text or ""))
                return result.text or ""
            elif result.reason == speechsdk.ResultReason.NoMatch:
                logging.warning("No speech could be recognized.")
                return ""
            else:
                logging.warning("Speech recognition failed or canceled: %s", result.reason)
                return ""
        else:
            logging.warning("Speech key/region not set; skipping speech SDK transcription.")
            return ""
    finally:
        try:
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
        except Exception:
            logging.exception("Failed to remove temp transcription file")


def parse_order(transcribed_text, openai_endpoint=None, openai_key=None, openai_deployment=None):
    """
    Parse transcribed_text into {'quantity': int|None, 'item': str|None}.
    - If Azure OpenAI config provided, uses the openai package configured for Azure.
    - Otherwise falls back to a regex parser.
    """
    logging.info("Parsing transcription: %s", transcribed_text)

    # Try Azure OpenAI via openai package if configured
    if openai_endpoint and openai_key and openai_deployment:
        try:
            try:
                import openai
            except ImportError:
                logging.warning("openai package is not installed; falling back to regex parser.")
                raise

            # Configure openai package for Azure usage
            openai.api_type = "azure"
            openai.api_base = openai_endpoint.rstrip("/")  # e.g. https://<resource>.openai.azure.com
            openai.api_version = "2023-05-15"  # update if your resource expects a different API version
            openai.api_key = openai_key

            system_msg = {
                "role": "system",
                "content": "You are a precise JSON generator. Respond with ONLY a single JSON object with keys: 'quantity' (integer or null) and 'item' (string or null). Respond with valid JSON only, no additional text."
            }
            user_msg = {
                "role": "user",
                "content": f"Extract quantity and item from this customer transcription: \"{transcribed_text}\". Example: {{\"quantity\": 3, \"item\": \"bottles\"}}"
            }

            resp = openai.ChatCompletion.create(
                engine=openai_deployment,
                messages=[system_msg, user_msg],
                max_tokens=80,
                temperature=0
            )

            # extract content robustly
            content = ""
            try:
                # response is usually a dict
                content = resp["choices"][0]["message"]["content"]
            except Exception:
                # fallback: stringify and try to find JSON inside
                content = str(resp)

            # Try parsing full content as JSON first
            obj = None
            try:
                obj = json.loads(content)
            except json.JSONDecodeError:
                # fallback: try to extract first {...} substring
                m = re.search(r"\{.*\}", content, re.DOTALL)
                if m:
                    try:
                        obj = json.loads(m.group(0))
                    except Exception:
                        logging.debug("Failed to json.loads extracted substring from OpenAI content.")
                        obj = None
                else:
                    logging.debug("OpenAI response did not contain a JSON object.")

            if obj:
                qty = obj.get("quantity")
                try:
                    qty = int(qty) if qty is not None else None
                except Exception:
                    qty = None
                item = obj.get("item")
                if isinstance(item, str):
                    item = item.strip().lower()
                return {"quantity": qty, "item": item}
            else:
                logging.warning("OpenAI response could not be parsed as JSON; falling back to regex parser.")
        except Exception:
            logging.exception("Azure OpenAI (openai package) parse failed; falling back to regex parser.")

    # Fallback regex-based parser
    try:
        # digits
        m = re.search(r"(\d+)\s+([A-Za-z\u0600-\u06FF][\w\u0600-\u06FF\s-]*)", transcribed_text, re.UNICODE)
        if m:
            return {"quantity": int(m.group(1)), "item": m.group(2).strip().lower()}
        # spelled numbers (basic)
        words_to_num = {"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10}
        m2 = re.search(r"\b(" + "|".join(words_to_num.keys()) + r")\b\s+([A-Za-z\u0600-\u06FF][\w\u0600-\u06FF\s-]*)", transcribed_text, re.IGNORECASE)
        if m2:
            return {"quantity": words_to_num.get(m2.group(1).lower()), "item": m2.group(2).strip().lower()}
    except Exception:
        logging.exception("Regex parsing failed")

    return {"quantity": None, "item": None}


def log_to_excel(order, customer_number, conn_str, container_name, excel_blob_name):
    logging.info("Logging order to Excel: %s", order)
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(excel_blob_name)

    download_file = os.path.join(tempfile.gettempdir(), f"orders_{uuid.uuid4().hex}.xlsx")
    try:
        logging.info("Downloading Excel blob %s from container %s", excel_blob_name, container_name)
        with open(download_file, "wb") as f:
            stream = blob_client.download_blob()
            f.write(stream.readall())
    except Exception as ex:
        logging.warning("Creating new workbook because download failed: %s", ex)
        wb = Workbook()
        ws = wb.active
        ws.append(["timestamp_utc", "customer_number", "item", "quantity"])
    else:
        wb = load_workbook(download_file)
        ws = wb.active

    ws.append([datetime.utcnow().isoformat(), customer_number, order.get("item"), order.get("quantity")])
    wb.save(download_file)

    with open(download_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info("Uploaded updated Excel to blob %s", excel_blob_name)

    try:
        if os.path.exists(download_file):
            os.remove(download_file)
    except Exception:
        logging.exception("Failed to cleanup excel temp file")

# Optional Twilio confirmation
def send_confirmation(to_number, order):
    from twilio.rest import Client
    TWILIO_SID = os.environ.get("TWILIO_SID")
    TWILIO_AUTH = os.environ.get("TWILIO_AUTH")
    TWILIO_WHATSAPP = os.environ.get("TWILIO_WHATSAPP_NUMBER")

    if not all([TWILIO_SID, TWILIO_AUTH, TWILIO_WHATSAPP]):
        logging.warning("Twilio config incomplete; skipping confirmation message.")
        return

    client = Client(TWILIO_SID, TWILIO_AUTH)
    qty = order.get("quantity") or ""
    item = order.get("item") or ""
    message = f"Your order of {qty} {item} has been logged."
    client.messages.create(
        body=message,
        from_=f"whatsapp:{TWILIO_WHATSAPP}",
        to=f"whatsapp:{to_number}"
    )