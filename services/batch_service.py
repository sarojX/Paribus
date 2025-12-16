from typing import List, Dict, Any, Optional
import asyncio
import time
import httpx
from config import HOSPITAL_API_BASE, HTTPX_TIMEOUT_SECONDS, MAX_HOSPITALS


class BatchServiceInterface:
    async def start_batch(self, batch_id: str, rows: List[Dict[str, Any]]) -> str:
        raise NotImplementedError()

    async def get_status(self, batch_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()

    async def resume_batch(self, batch_id: str) -> Dict[str, Any]:
        raise NotImplementedError()


class HospitalBatchService(BatchServiceInterface):
    """Service responsible for batch processing and progress tracking."""

    def __init__(self):
        # in-memory stores; replace with Redis/DB for production
        self.batch_progress: Dict[str, Dict] = {}
        self.batch_subscribers: Dict[str, List[asyncio.Queue]] = {}

    async def start_batch(self, batch_id: str, rows: List[Dict[str, Any]]) -> str:
        self.batch_progress[batch_id] = {
            "batch_id": batch_id,
            "total_hospitals": len(rows),
            "processed_hospitals": 0,
            "failed_hospitals": 0,
            "processing_time_seconds": None,
            "batch_activated": False,
            "hospitals": [],
            "status": "processing",
            "started_at": time.time(),
        }
        asyncio.create_task(self._process_batch(batch_id, rows))
        return batch_id

    async def get_status(self, batch_id: str) -> Optional[Dict[str, Any]]:
        return self.batch_progress.get(batch_id)

    async def resume_batch(self, batch_id: str) -> Dict[str, Any]:
        data = self.batch_progress.get(batch_id)
        if not data:
            raise KeyError("batch not found")
        if data.get("status") == "processing":
            raise RuntimeError("batch already processing")

        to_retry = []
        for entry in data.get("hospitals", []):
            st = entry.get("status", "")
            if entry.get("payload") and st not in ("created", "created_and_activated", "invalid_row_missing_name_or_address"):
                to_retry.append(entry)

        if not to_retry:
            return {"batch_id": batch_id, "message": "nothing_to_retry"}

        data["status"] = "processing"
        asyncio.create_task(self._process_batch_retry(batch_id, to_retry))
        return {"batch_id": batch_id, "retry_count": len(to_retry), "status": "retry_scheduled"}

    # --- internal methods ---
    async def _process_batch(self, batch_id: str, rows: List[Dict[str, Any]]):
        started = time.time()
        processed = 0
        failed = 0

        async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
            for idx, row in enumerate(rows, start=1):
                name = (row.get("name") or "").strip()
                address = (row.get("address") or "").strip()
                phone = (row.get("phone") or "").strip() if "phone" in row else None

                payload = {"name": name, "address": address}
                if phone:
                    payload["phone"] = phone

                if not name or not address:
                    failed += 1
                    entry = {"row": idx, "hospital_id": None, "name": name or None, "status": "invalid_row_missing_name_or_address", "payload": payload}
                    self.batch_progress[batch_id]["hospitals"].append(entry)
                    self.batch_progress[batch_id]["failed_hospitals"] = failed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry})
                    continue

                send_payload = {**payload, "creation_batch_id": batch_id}

                try:
                    resp = await client.post(f"{HOSPITAL_API_BASE}/hospitals/", json=send_payload)
                except httpx.RequestError as exc:
                    failed += 1
                    entry = {"row": idx, "hospital_id": None, "name": name, "status": f"request_error: {str(exc)}", "payload": payload}
                    self.batch_progress[batch_id]["hospitals"].append(entry)
                    self.batch_progress[batch_id]["failed_hospitals"] = failed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry})
                    continue

                if resp.status_code in (200, 201):
                    processed += 1
                    data = resp.json()
                    entry = {"row": idx, "hospital_id": data.get("id"), "name": name, "status": "created", "payload": payload}
                    self.batch_progress[batch_id]["hospitals"].append(entry)
                    self.batch_progress[batch_id]["processed_hospitals"] = processed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry})
                else:
                    failed += 1
                    try:
                        err = resp.json()
                    except Exception:
                        err = {"status_code": resp.status_code, "text": resp.text}
                    entry = {"row": idx, "hospital_id": None, "name": name, "status": "create_failed", "error": err, "payload": payload}
                    self.batch_progress[batch_id]["hospitals"].append(entry)
                    self.batch_progress[batch_id]["failed_hospitals"] = failed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry})

        batch_activated = False
        if failed == 0:
            async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
                try:
                    act_resp = await client.patch(f"{HOSPITAL_API_BASE}/hospitals/batch/{batch_id}/activate")
                    batch_activated = act_resp.status_code in (200, 204)
                    if batch_activated:
                        for r in self.batch_progress[batch_id]["hospitals"]:
                            if r.get("status") == "created":
                                r["status"] = "created_and_activated"
                        await self._broadcast_progress(batch_id, {"type": "batch_activated", "data": {"batch_activated": True}})
                except Exception:
                    batch_activated = False

        finished = time.time()
        processing_time = int(finished - started)

        self.batch_progress[batch_id].update({
            "processing_time_seconds": processing_time,
            "batch_activated": batch_activated,
            "status": "completed",
            "finished_at": finished,
        })

        await self._broadcast_progress(batch_id, {"type": "completed", "data": self.batch_progress[batch_id]})

    async def _process_batch_retry(self, batch_id: str, entries: List[Dict[str, Any]]):
        processed = self.batch_progress[batch_id].get("processed_hospitals", 0)
        failed = self.batch_progress[batch_id].get("failed_hospitals", 0)

        async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
            for entry in entries:
                idx = entry.get("row")
                payload = entry.get("payload") or {}
                send_payload = {**payload, "creation_batch_id": batch_id}

                try:
                    resp = await client.post(f"{HOSPITAL_API_BASE}/hospitals/", json=send_payload)
                except httpx.RequestError as exc:
                    failed += 1
                    entry_update = {"row": idx, "hospital_id": None, "name": payload.get("name"), "status": f"request_error: {str(exc)}", "payload": payload}
                    self._update_stored_entry(batch_id, idx, entry_update)
                    self.batch_progress[batch_id]["failed_hospitals"] = failed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry_update})
                    continue

                if resp.status_code in (200, 201):
                    processed += 1
                    data_resp = resp.json()
                    entry_update = {"row": idx, "hospital_id": data_resp.get("id"), "name": payload.get("name"), "status": "created", "payload": payload}
                    self._update_stored_entry(batch_id, idx, entry_update)
                    self.batch_progress[batch_id]["processed_hospitals"] = processed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry_update})
                else:
                    failed += 1
                    try:
                        err = resp.json()
                    except Exception:
                        err = {"status_code": resp.status_code, "text": resp.text}
                    entry_update = {"row": idx, "hospital_id": None, "name": payload.get("name"), "status": "create_failed", "error": err, "payload": payload}
                    self._update_stored_entry(batch_id, idx, entry_update)
                    self.batch_progress[batch_id]["failed_hospitals"] = failed
                    await self._broadcast_progress(batch_id, {"type": "row_update", "data": entry_update})

        remaining_failures = sum(1 for r in self.batch_progress[batch_id]["hospitals"] if r.get("status") not in ("created", "created_and_activated"))
        batch_activated = False
        if remaining_failures == 0:
            async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT_SECONDS) as client:
                try:
                    act_resp = await client.patch(f"{HOSPITAL_API_BASE}/hospitals/batch/{batch_id}/activate")
                    batch_activated = act_resp.status_code in (200, 204)
                    if batch_activated:
                        for r in self.batch_progress[batch_id]["hospitals"]:
                            if r.get("status") == "created":
                                r["status"] = "created_and_activated"
                        await self._broadcast_progress(batch_id, {"type": "batch_activated", "data": {"batch_activated": True}})
                except Exception:
                    batch_activated = False

        self.batch_progress[batch_id].update({
            "batch_activated": batch_activated,
            "failed_hospitals": self.batch_progress[batch_id].get("failed_hospitals", failed),
            "processed_hospitals": self.batch_progress[batch_id].get("processed_hospitals", processed),
            "status": "completed",
        })

        await self._broadcast_progress(batch_id, {"type": "completed", "data": self.batch_progress[batch_id]})

    def _update_stored_entry(self, batch_id: str, row_idx: int, new_entry: Dict[str, Any]):
        lst = self.batch_progress[batch_id].get("hospitals", [])
        for i, e in enumerate(lst):
            if e.get("row") == row_idx:
                lst[i] = {**e, **new_entry}
                return

    async def _broadcast_progress(self, batch_id: str, message: Dict) -> None:
        queues = self.batch_subscribers.get(batch_id, [])
        for q in list(queues):
            try:
                await q.put(message)
            except Exception:
                pass

    # WebSocket subscription helpers
    def subscribe(self, batch_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.batch_subscribers.setdefault(batch_id, []).append(q)
        return q

    def unsubscribe(self, batch_id: str, q: asyncio.Queue) -> None:
        subs = self.batch_subscribers.get(batch_id)
        if subs and q in subs:
            subs.remove(q)