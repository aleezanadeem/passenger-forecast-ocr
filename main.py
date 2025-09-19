import os
import io
import re
import cv2
import pytesseract
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from gspread_dataframe import set_with_dataframe
import json

# ========= CONFIG =========
RAW_FOLDER_ID = "1BhB3pZWxOMcTXZcmYzrxwtgNwp8dg9pX"   # <-- replace with your RAW folder ID
SHEET_NAME = "Passenger Forecast Data"                # <-- change to your target sheet name
LOG_SHEET_NAME = "Log"                                # sheet tab for processed files
# ==========================

# --- Authenticate with service account from GitHub secrets ---
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
)

# --- Google APIs clients ---
drive_service = build("drive", "v3", credentials=creds)
gc = gspread.authorize(creds)

# --- Open target sheet ---
sh = gc.open(SHEET_NAME)
worksheet = sh.sheet1
try:
    log_ws = sh.worksheet(LOG_SHEET_NAME)
except gspread.WorksheetNotFound:
    log_ws = sh.add_worksheet(title=LOG_SHEET_NAME, rows=100, cols=2)
    log_ws.append_row(["Filename", "Processed At"])

processed_files = {row[0] for row in log_ws.get_all_values()[1:]}  # skip header

# --- Tesseract config ---
pytesseract.pytesseract.tesseract_cmd = r"/usr/bin/tesseract"

# --- Get latest PNG file in RAW folder ---
results = drive_service.files().list(
    q=f"'{RAW_FOLDER_ID}' in parents and trashed=false and mimeType contains 'image/png'",
    orderBy="modifiedTime desc",
    pageSize=1,
    fields="files(id, name, modifiedTime)"
).execute()

files = results.get("files", [])
if not files:
    print("⚠️ No PNG files found.")
    exit()

file = files[0]
file_id, filename = file["id"], file["name"]

if filename in processed_files:
    print(f"⏩ Skipping already processed: {filename}")
    exit()

print(f"📥 Processing {filename}")

# --- Download file from Drive ---
request = drive_service.files().get_media(fileId=file_id)
fh = io.BytesIO(request.execute())
with open(filename, "wb") as f:
    f.write(fh.getbuffer())

# --- OCR with Tesseract ---
img = cv2.imread(filename)
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
custom_config = r'--oem 3 --psm 6'
text = pytesseract.image_to_string(gray, config=custom_config)

lines = text.split("\n")
date_pattern = r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),"

rows = []
for line in lines:
    if re.match(date_pattern, line.strip()):
        parts = line.split()
        rows.append(parts)

# Extract submission date
submission_pattern = r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(AM|PM)"
submission_date = None
for line in lines:
    match = re.search(submission_pattern, line)
    if match:
        submission_date = match.group(0)
        break

# Headers
headers = [
    "Day", "Month", "Date", "Year", "Submission_Date",
    "TSA_Domestic", "TSA_International", "O&D", "Connecting",
    "Total_Passengers", "Scheduled_Seats", "Load_Factor",
    "T_Con", "A_Con", "B_Con", "C_Con", "D_Con", "E_Con", "F_Con"
]

cleaned_rows = []
for row in rows:
    day = row[0].replace(",", "")
    month = row[1]
    date = row[2].replace(",", "")
    year = row[3]
    numbers = row[4:18]  # expected 14 numbers
    cleaned_rows.append([day, month, date, year, submission_date] + numbers)

df = pd.DataFrame(cleaned_rows, columns=headers)

# --- Append to Google Sheet ---
existing = worksheet.get_all_values()
if not existing:
    set_with_dataframe(worksheet, df)
else:
    worksheet.add_rows(df.shape[0])
    set_with_dataframe(worksheet, df, row=len(existing) + 1, include_column_header=False)

# --- Log processed file ---
log_ws.append_row([filename, str(pd.Timestamp.now())])
print(f"✅ Done with {filename}")
