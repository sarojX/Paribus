import pytest
import asyncio
import re
from httpx import Response
import respx

from services.batch_service import HospitalBatchService


@pytest.mark.asyncio
async def test_start_batch_success(respx_mock):
    service = HospitalBatchService()
    rows = [{"name": "A", "address": "Addr A"}, {"name": "B", "address": "Addr B"}]

    # Mock POST to return 201 for any hospital create
    respx_mock.post("https://hospital-directory.onrender.com/hospitals/").respond(201, json={"id": 1})
    # Mock PATCH activate
    respx_mock.patch(re.compile(r"https://hospital-directory.onrender.com/hospitals/batch/.*/activate")).respond(200)

    batch_id = "batch-success-1"
    await service.start_batch(batch_id, rows)

    # wait for completion (timeout after 5s)
    status = None
    for _ in range(50):
        status = await service.get_status(batch_id)
        if status and status.get("status") == "completed":
            break
        await asyncio.sleep(0.1)

    assert status is not None
    assert status["processed_hospitals"] == 2
    assert status["failed_hospitals"] == 0
    assert status["batch_activated"] is True


@pytest.mark.asyncio
async def test_start_batch_failure_and_resume(respx_mock):
    service = HospitalBatchService()
    rows = [{"name": "ok", "address": "Addr"}, {"name": "fail", "address": "Addr"}]

    # Callback for POST: fail when name == 'fail'
    def post_callback(request):
        body = request.json()
        if body.get("name") == "fail":
            return Response(500, json={"detail": "server error"})
        return Response(201, json={"id": 123})

    respx_mock.post("https://hospital-directory.onrender.com/hospitals/").mock(side_effect=post_callback)

    # activation should not be called because there will be a failure
    respx_mock.patch(re.compile(r"https://hospital-directory.onrender.com/hospitals/batch/.*/activate")).respond(200)

    batch_id = "batch-fail-1"
    await service.start_batch(batch_id, rows)

    # wait for completion
    status = None
    for _ in range(50):
        status = await service.get_status(batch_id)
        if status and status.get("status") == "completed":
            break
        await asyncio.sleep(0.1)

    assert status is not None
    assert status["processed_hospitals"] == 1
    assert status["failed_hospitals"] == 1
    assert status["batch_activated"] is False

    # Now mock POST to succeed for retry
    def post_callback_retry(request):
        return Response(201, json={"id": 999})

    respx_mock.post("https://hospital-directory.onrender.com/hospitals/").mock(side_effect=post_callback_retry)
    # activation success
    respx_mock.patch(re.compile(r"https://hospital-directory.onrender.com/hospitals/batch/.*/activate")).respond(200)

    result = await service.resume_batch(batch_id)
    assert result.get("status") == "retry_scheduled"

    # wait for completion again
    for _ in range(50):
        status = await service.get_status(batch_id)
        if status and status.get("status") == "completed":
            break
        await asyncio.sleep(0.1)

    assert status["failed_hospitals"] == 0
    assert status["processed_hospitals"] == 2
    assert status["batch_activated"] is True
