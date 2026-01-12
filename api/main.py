from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import List
import os
import boto3
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="CSV Folder Upload API")

s3_client = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
print(f"Using S3 Bucket: {BUCKET_NAME}")

@app.post("/upload/csv-folder")
async def upload_csv_folder(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado")

    uploaded_files = []

    for file in files:
        if not file.filename.endswith(".csv"):
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo inválido: {file.filename}"
            )

        try:
            s3_client.upload_fileobj(
                file.file,
                BUCKET_NAME,
                f"uploads/{file.filename}",
                ExtraArgs={"ContentType": "text/csv"}
            )
            uploaded_files.append(file.filename)

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao enviar {file.filename}: {str(e)}"
            )

    return {
        "message": "Upload realizado com sucesso",
        "files": uploaded_files
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)