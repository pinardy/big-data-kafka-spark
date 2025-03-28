from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from typing import List
import pandas as pd
import io

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("static/upload.html")

@app.post("/upload-multiple/")
async def upload_multiple(files: List[UploadFile] = File(...)):
    combined_data = {}

    for file in files:
        contents = await file.read()
        try:
            df = pd.read_csv(io.BytesIO(contents))
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": f" {file.filename} fail: {str(e)}"})

        if "bookingID" not in df.columns:
            return JSONResponse(status_code=400, content={"error": f" {file.filename} lost 'bookingID' column"})

        for booking_id, group in df.groupby("bookingID"):
            tid = str(booking_id)
            if tid not in combined_data:
                combined_data[tid] = []
            combined_data[tid].extend(group.to_dict(orient="records"))

    return JSONResponse(content=combined_data)
