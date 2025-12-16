import pytest
import asyncio
from httpx import AsyncClient
from main import app as fastapi_app


@pytest.fixture
async def async_client():
    async with AsyncClient(app=fastapi_app, base_url="http://testserver") as ac:
        yield ac


@pytest.fixture
def anyio_backend():
    return "asyncio"
