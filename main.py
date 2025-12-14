from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import csv
import io
import uuid
import time
import httpx
from typing import List, Dict, Any
from config import HOSPITAL_API_BASE, MAX_HOSPITALS, HTTPX_TIMEOUT_SECONDS

app = FastAPI(title="Hospital Bulk Import API")

@app.post("/hospitals/bulk")
async def bulk_create_hospitals(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV file as UTF-8")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    total = len(rows)
    if total == 0:
        raise HTTPException(status_code=400, detail="CSV is empty or missing header row")
    if total > MAX_HOSPITALS:
        raise HTTPException(status_code=400, detail=f"Maximum {MAX_HOSPITALS} hospitals allowed per upload")

    batch_id = str(uuid.uuid4())
    started = time.time()

    results: List[Dict[str, Any]] = []
    processed = 0
    failed = 0

    async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
        for idx, row in enumerate(rows, start=1):
            name = (row.get("name") or "").strip()
            address = (row.get("address") or "").strip()
            phone = (row.get("phone") or "").strip() if "phone" in row else None

            if not name or not address:
                failed += 1
                results.append({"row": idx, "hospital_id": None, "name": name or None, "status": "invalid_row_missing_name_or_address"})
                continue

            payload = {
                "name": name,
                "address": address,
                "creation_batch_id": batch_id,
            }
            if phone:
                payload["phone"] = phone

            try:
                resp = await client.post(f"{HOSPITAL_API_BASE}/hospitals/", json=payload)
            except httpx.RequestError as exc:
                failed += 1
                results.append({"row": idx, "hospital_id": None, "name": name, "status": f"request_error: {str(exc)}"})
                continue

            if resp.status_code in (200, 201):
                processed += 1
                data = resp.json()
                results.append({"row": idx, "hospital_id": data.get("id"), "name": name, "status": "created"})
            else:
                failed += 1
                try:
                    err = resp.json()
                except Exception:
                    err = {"status_code": resp.status_code, "text": resp.text}
                results.append({"row": idx, "hospital_id": None, "name": name, "status": "create_failed", "error": err})

    batch_activated = False
    if failed == 0:
        async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
            try:
                act_resp = await client.patch(f"{HOSPITAL_API_BASE}/hospitals/batch/{batch_id}/activate")
                batch_activated = act_resp.status_code in (200, 204)
                if batch_activated:
                    for r in results:
                        if r.get("status") == "created":
                            r["status"] = "created_and_activated"
            except Exception:
                batch_activated = False

    finished = time.time()
    processing_time = int(finished - started)

    response = {
        "batch_id": batch_id,
        "total_hospitals": total,
        "processed_hospitals": processed,
        "failed_hospitals": failed,
        "processing_time_seconds": processing_time,
        "batch_activated": batch_activated,
        "hospitals": results,
    }

    return JSONResponse(status_code=200, content=response)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
