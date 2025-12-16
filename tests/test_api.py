import pytest
import asyncio
import io

from httpx import Response
import respx


@pytest.mark.asyncio
async def test_bulk_endpoint_integration(async_client, respx_mock):
    # Prepare a CSV with two rows
    csv_content = "name,address,phone\nAlpha,Addr1,111\nBeta,Addr2,222\n"

    # Mock external API create and activation
    respx_mock.post("https://hospital-directory.onrender.com/hospitals/").respond(201, json={"id": 10})
    respx_mock.patch(respx.MockRouter.re.compile(r"https://hospital-directory.onrender.com/hospitals/batch/.*/activate")).respond(200)

    files = {"file": ("hospitals.csv", csv_content, "text/csv")}
    resp = await async_client.post("/hospitals/bulk", files=files)
    assert resp.status_code == 202
    data = resp.json()
    batch_id = data.get("batch_id")
    assert batch_id

    # poll for status
    status = None
    for _ in range(50):
        sresp = await async_client.get(f"/hospitals/batch/{batch_id}/status")
        if sresp.status_code == 200:
            status = sresp.json()
            if status.get("status") == "completed":
                break
        await asyncio.sleep(0.1)

    assert status is not None
    assert status["processed_hospitals"] == 2
    assert status["failed_hospitals"] == 0
