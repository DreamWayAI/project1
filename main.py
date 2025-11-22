import io
import os
from fastapi import FastAPI
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
import boto3
from botocore.client import Config

app = FastAPI()

SERVICE_ACCOUNT_FILE = "service.json"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version="s3v4")
)

class FileRequest(BaseModel):
    fileId: str
    filename: str

@app.post("/upload")
async def upload_to_s3(req: FileRequest):
    request = drive_service.files().get_media(fileId=req.fileId)
    resp, content = drive_service._http.request(request.uri)

    if resp.status != 200:
        return {"error": "Google Drive download failed", "status": resp.status}

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=req.filename,
        Body=content,
        ContentType="video/mp4"
    )

    file_url = f"{S3_ENDPOINT}/{S3_BUCKET}/{req.filename}"
    return {"url": file_url}
