import os
import json
import threading
from datetime import datetime
from dotenv import load_dotenv

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
    import io
except ImportError:
    gspread = None
    Credentials = None
    build = None
    MediaIoBaseUpload = None
    io = None

load_dotenv()

# Lock for thread-safe operations
_google_lock = threading.Lock()

# Google Service Account Credentials
_service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
_sheet_id_trades = os.getenv('GOOGLE_SHEET_ID_TRADES')
_sheet_id_signals = os.getenv('GOOGLE_SHEET_ID_SIGNALS')
_drive_log_folder_id = os.getenv('GOOGLE_DRIVE_LOG_FOLDER_ID')

# Global clients (lazy initialized)
_gspread_client = None
_drive_service = None


def _get_credentials():
    if not _service_account_json:
        return None
    try:
        service_account_info = json.loads(_service_account_json)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file'
        ]
        return Credentials.from_service_account_info(service_account_info, scopes=scopes)
    except Exception as e:
        print(f"[Google Integration] Error getting credentials: {e}")
        return None


def _get_gspread_client():
    global _gspread_client
    with _google_lock:
        if _gspread_client is None:
            credentials = _get_credentials()
            if credentials and gspread:
                try:
                    _gspread_client = gspread.authorize(credentials)
                except Exception as e:
                    print(f"[Google Integration] Error initializing gspread: {e}")
        return _gspread_client


def _get_drive_service():
    global _drive_service
    with _google_lock:
        if _drive_service is None:
            credentials = _get_credentials()
            if credentials and build:
                try:
                    _drive_service = build('drive', 'v3', credentials=credentials)
                except Exception as e:
                    print(f"[Google Integration] Error initializing Drive service: {e}")
        return _drive_service


def append_trade_to_sheet(trade_data):
    """
    Append a trade row to Google Sheets (tardes988)
    trade_data: dict with keys matching closed_positions table
    """
    if not _sheet_id_trades:
        return False
    client = _get_gspread_client()
    if not client:
        return False
    try:
        sheet = client.open_by_key(_sheet_id_trades)
        worksheet = sheet.sheet1
        
        # Check if headers exist; if not, create them
        headers = worksheet.row_values(1)
        expected_headers = [
            'id', 'symbol', 'side', 'entry_price', 'exit_price', 'quantity', 
            'pnl', 'entry_time', 'exit_time', 'is_latest', 'entry_order_id', 
            'exit_order_id', 'trade_decisions', 'created_at'
        ]
        if not headers or headers != expected_headers:
            worksheet.insert_row(expected_headers, 1)
        
        # Prepare row data
        row = [
            trade_data.get('id'),
            trade_data.get('symbol'),
            trade_data.get('side'),
            trade_data.get('entry_price'),
            trade_data.get('exit_price'),
            trade_data.get('quantity'),
            trade_data.get('pnl'),
            trade_data.get('entry_time'),
            trade_data.get('exit_time'),
            trade_data.get('is_latest'),
            trade_data.get('entry_order_id'),
            trade_data.get('exit_order_id'),
            trade_data.get('trade_decisions'),
            trade_data.get('created_at')
        ]
        
        worksheet.append_row(row, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        print(f"[Google Integration] Error appending trade to sheet: {e}")
        return False


def append_signal_to_sheet(signal_data):
    """
    Append a signal row to Google Sheets (signals9888)
    signal_data: dict with keys matching signal_scores_history table
    """
    if not _sheet_id_signals:
        return False
    client = _get_gspread_client()
    if not client:
        return False
    try:
        sheet = client.open_by_key(_sheet_id_signals)
        worksheet = sheet.sheet1
        
        # Check if headers exist; if not, create them
        headers = worksheet.row_values(1)
        expected_headers = [
            'id', 'order_id', 'symbol', 'position', 'lot_size', 'tf_1m', 'tf_5m', 
            'tf_15m', 'tf_30m', 'tf_1h', 'tf_2h', 'tf_4h', 'tf_1d', 'tf_1w', 
            'final_decision', 'entry_time', 'created_at'
        ]
        if not headers or headers != expected_headers:
            worksheet.insert_row(expected_headers, 1)
        
        # Prepare row data
        row = [
            signal_data.get('id'),
            signal_data.get('order_id'),
            signal_data.get('symbol'),
            signal_data.get('position'),
            signal_data.get('lot_size'),
            signal_data.get('tf_1m'),
            signal_data.get('tf_5m'),
            signal_data.get('tf_15m'),
            signal_data.get('tf_30m'),
            signal_data.get('tf_1h'),
            signal_data.get('tf_2h'),
            signal_data.get('tf_4h'),
            signal_data.get('tf_1d'),
            signal_data.get('tf_1w'),
            signal_data.get('final_decision'),
            signal_data.get('entry_time'),
            signal_data.get('created_at')
        ]
        
        worksheet.append_row(row, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        print(f"[Google Integration] Error appending signal to sheet: {e}")
        return False


def append_log_to_drive(log_filename, log_content):
    """
    Append log content to Google Drive log file (creates file if doesn't exist)
    """
    if not _drive_log_folder_id:
        return False
    service = _get_drive_service()
    if not service:
        return False
    try:
        # Check if file exists in folder
        query = f"name='{log_filename}' and '{_drive_log_folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = results.get('files', [])
        
        log_content_bytes = log_content.encode('utf-8')
        
        if files:
            # Update existing file
            file_id = files[0]['id']
            # Download existing content first
            request = service.files().get_media(fileId=file_id)
            existing_content = io.BytesIO()
            downloader = MediaIoBaseDownload(existing_content, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            existing_content.seek(0)
            combined_content = existing_content.read() + log_content_bytes
            
            media = MediaIoBaseUpload(io.BytesIO(combined_content), mimetype='text/plain', resumable=True)
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            # Create new file
            file_metadata = {
                'name': log_filename,
                'parents': [_drive_log_folder_id]
            }
            media = MediaIoBaseUpload(io.BytesIO(log_content_bytes), mimetype='text/plain', resumable=True)
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        return True
    except Exception as e:
        print(f"[Google Integration] Error appending log to Drive: {e}")
        return False


def get_trades_from_sheet(limit=100, offset=0, search=None, sort_by='created_at', sort_order='desc'):
    """
    Get trades from Google Sheet (for UI)
    Returns list of dicts
    """
    if not _sheet_id_trades:
        return []
    client = _get_gspread_client()
    if not client:
        return []
    try:
        sheet = client.open_by_key(_sheet_id_trades)
        worksheet = sheet.sheet1
        all_records = worksheet.get_all_records()
        
        # Sort
        if sort_by in all_records[0] if all_records else False:
            all_records.sort(key=lambda x: x.get(sort_by, ''), reverse=(sort_order == 'desc'))
        
        # Search
        if search:
            search_lower = search.lower()
            all_records = [
                r for r in all_records 
                if any(search_lower in str(v).lower() for v in r.values())
            ]
        
        # Pagination
        return all_records[offset:offset+limit]
    except Exception as e:
        print(f"[Google Integration] Error getting trades from sheet: {e}")
        return []


def get_signals_from_sheet(limit=100, offset=0, search=None, sort_by='created_at', sort_order='desc'):
    """
    Get signals from Google Sheet (for UI)
    Returns list of dicts
    """
    if not _sheet_id_signals:
        return []
    client = _get_gspread_client()
    if not client:
        return []
    try:
        sheet = client.open_by_key(_sheet_id_signals)
        worksheet = sheet.sheet1
        all_records = worksheet.get_all_records()
        
        # Sort
        if sort_by in all_records[0] if all_records else False:
            all_records.sort(key=lambda x: x.get(sort_by, ''), reverse=(sort_order == 'desc'))
        
        # Search
        if search:
            search_lower = search.lower()
            all_records = [
                r for r in all_records 
                if any(search_lower in str(v).lower() for v in r.values())
            ]
        
        # Pagination
        return all_records[offset:offset+limit]
    except Exception as e:
        print(f"[Google Integration] Error getting signals from sheet: {e}")
        return []
