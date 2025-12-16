from fastapi import FastAPI, File, UploadFile, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import csv
import io
import uuid
import time
import asyncio
from typing import List, Dict, Any
from config import MAX_HOSPITALS
from services.batch_service import HospitalBatchService

app = FastAPI(title="Hospital Bulk Import API")

# remove
# In-memory stores for progress and websocket subscribers. For real deployments
# this should be replaced with a persistent store (Redis, DB) and a pub/sub system.
batch_service = HospitalBatchService()


@app.post("/hospitals/bulk")
async def bulk_create_hospitals(file: UploadFile = File(...)):
    """Starts bulk processing in background and returns immediately with `batch_id` and totals.

    Real-time updates are available via WebSocket `/ws/batch/{batch_id}` or polling `/hospitals/batch/{batch_id}/status`.
    """
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
    await batch_service.start_batch(batch_id, rows)
    return JSONResponse(status_code=202, content={"batch_id": batch_id, "total_hospitals": total, "status": "started"})


@app.post("/hospitals/batch/{batch_id}/resume")
async def resume_batch(batch_id: str):
    """Resume processing for a batch: retry failed creates.

    Delegates to the BatchService which schedules retry work in background.
    """
    try:
        result = await batch_service.resume_batch(batch_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Batch not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Batch is already processing")
    return JSONResponse(status_code=202 if result.get("status") == "retry_scheduled" else 200, content=result)


@app.post("/hospitals/validate")
async def validate_csv(file: UploadFile = File(...)):
    """Validate CSV structure and per-row required fields without creating hospitals.

    Returns a summary including total rows, valid rows, invalid rows and a list of per-row issues.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV file as UTF-8")

    stream = io.StringIO(text)
    reader = csv.DictReader(stream)
    headers = reader.fieldnames or []
    required_headers = ["name", "address"]
    missing_headers = [h for h in required_headers if h not in [x.strip() for x in headers]]
    if missing_headers:
        return JSONResponse(status_code=400, content={
            "ok": False,
            "error": "missing_required_headers",
            "missing_headers": missing_headers,
            "expected_headers": required_headers,
        })

    rows = list(reader)
    total = len(rows)
    if total == 0:
        raise HTTPException(status_code=400, detail="CSV has headers but contains no data rows")
    if total > MAX_HOSPITALS:
        return JSONResponse(status_code=400, content={
            "ok": False,
            "error": "too_many_rows",
            "total_rows": total,
            "max_allowed": MAX_HOSPITALS,
        })

    issues = []
    valid_count = 0
    seen_names = set()
    for idx, row in enumerate(rows, start=1):
        row_issues: List[str] = []
        name = (row.get("name") or "").strip()
        address = (row.get("address") or "").strip()
        if not name:
            row_issues.append("missing_name")
        if not address:
            row_issues.append("missing_address")
        if name:
            if name in seen_names:
                row_issues.append("duplicate_name")
            else:
                seen_names.add(name)

        if row_issues:
            issues.append({"row": idx, "issues": row_issues, "name": name or None})
        else:
            valid_count += 1

    result = {
        "ok": True,
        "total_rows": total,
        "valid_rows": valid_count,
        "invalid_rows": len(issues),
        "issues": issues,
        "max_allowed": MAX_HOSPITALS,
    }

    return JSONResponse(status_code=200, content=result)


@app.get("/hospitals/batch/{batch_id}/status")
async def get_batch_status(batch_id: str):
    """Polling endpoint to get current batch progress."""
    data = await batch_service.get_status(batch_id)
    if not data:
        raise HTTPException(status_code=404, detail="Batch not found")
    return JSONResponse(status_code=200, content=data)


@app.websocket("/ws/batch/{batch_id}")
async def websocket_batch_progress(websocket: WebSocket, batch_id: str):
    """WebSocket endpoint that streams progress updates for a batch."""
    await websocket.accept()
    q = batch_service.subscribe(batch_id)
    try:
        current = await batch_service.get_status(batch_id)
        if current:
            await websocket.send_json({"type": "current", "data": current})

        while True:
            message = await q.get()
            await websocket.send_json(message)

    except WebSocketDisconnect:
        pass
    finally:
        batch_service.unsubscribe(batch_id, q)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
