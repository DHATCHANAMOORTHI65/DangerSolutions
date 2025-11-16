import datetime
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Danger Solutions Backend Running Successfully!"}

@app.get("/status")
def status():
    return {"status": "OK", "version": "1.0"}

from fastapi import File, UploadFile
import pandas as pd
import os

RAW_DATA_FOLDER = "raw_data"

# Upload Endpoint
@app.post("/upload-data")
async def upload_data(file: UploadFile = File(...)):
    # File type validation
    if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx")):
        return {"error": "Only CSV or Excel files are allowed"}

    # Save raw file
    file_location = os.path.join(RAW_DATA_FOLDER, file.filename)

    with open(file_location, "wb") as buffer:
        buffer.write(await file.read())

    write_log(f"Uploaded file: {file.filename}")
    return {
        "filename": file.filename,
        "message": "File uploaded successfully",
        "path": file_location
    }

CLEANED_DATA_FOLDER = "cleaned_data"

def clean_dataframe(df: pd.DataFrame):
    # Remove completely empty rows
    df = df.dropna(how="all")

    # Strip extra spaces from all string columns
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Convert column names to lowercase and replace spaces with _
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Fill missing values (optional)
    df = df.fillna("NULL")

    return df


@app.post("/clean-data")
async def clean_data(filename: str):
    raw_file_path = os.path.join(RAW_DATA_FOLDER, filename)

    if not os.path.exists(raw_file_path):
        return {"error": "File not found in raw_data folder"}

    # Read file based on type
    if filename.endswith(".csv"):
        df = pd.read_csv(raw_file_path)
    else:
        df = pd.read_excel(raw_file_path)

    # Clean the dataframe
    cleaned_df = clean_dataframe(df)

    # Generate cleaned file path
    cleaned_file_path = os.path.join(CLEANED_DATA_FOLDER, filename)

    # Save cleaned CSV
    cleaned_df.to_csv(cleaned_file_path, index=False)

    write_log(f"Cleaned file: {filename}")
    # Return cleaned data as JSON
    return {
        "message": "File cleaned successfully",
        "cleaned_file": cleaned_file_path,
        "preview": cleaned_df.head(10).to_dict(orient="records")
    }

@app.get("/view-cleaned-data")
async def view_cleaned_data(filename: str):
    cleaned_file_path = os.path.join(CLEANED_DATA_FOLDER, filename)

    if not os.path.exists(cleaned_file_path):
        return {"error": "Cleaned file not found"}

    try:
        # Try reading with UTF-8
        df = pd.read_csv(cleaned_file_path, encoding="utf-8")
    except Exception:
        # Try fallback encoding
        df = pd.read_csv(cleaned_file_path, encoding="latin1")

    # FIX: Replace NaN, NaT, None, Inf
    df = df.replace([float("inf"), float("-inf")], "NULL")
    df = df.fillna("NULL")

    # Convert all values to strings to avoid JSON errors
    df = df.astype(str)

    return {
        "filename": filename,
        "total_rows": len(df),
        "columns": list(df.columns),
        "data_preview": df.head(20).to_dict(orient="records")
    }




def write_log(message):
    log_file = os.path.join("logs", "app.log")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file, "a") as f:
        f.write(f"{timestamp} - {message}\n")

from fastapi.responses import FileResponse

@app.get("/download-cleaned-file")
async def download_cleaned_file(filename: str):
    cleaned_file_path = os.path.join(CLEANED_DATA_FOLDER, filename)

    if not os.path.exists(cleaned_file_path):
        return {"error": "File not found"}

    return FileResponse(cleaned_file_path, filename=filename)

@app.get("/list-files")
def list_files():
    raw_files = os.listdir(RAW_DATA_FOLDER)
    cleaned_files = os.listdir(CLEANED_DATA_FOLDER)

    return {
        "raw_files": raw_files,
        "cleaned_files": cleaned_files
    }

@app.get("/view-logs")
def view_logs():
    log_file = os.path.join("logs", "app.log")

    if not os.path.exists(log_file):
        return {"error": "Log file not found"}

    with open(log_file, "r") as f:
        lines = f.readlines()

    return {"logs": lines[-50:]} 
