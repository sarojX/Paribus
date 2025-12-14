import os

# Config values are loaded from environment variables when available so
# they can be configured at deploy time (Render, Docker, etc.).
HOSPITAL_API_BASE = os.getenv("HOSPITAL_API_BASE", "https://hospital-directory.onrender.com")

try:
	MAX_HOSPITALS = int(os.getenv("MAX_HOSPITALS", "20"))
except (TypeError, ValueError):
	MAX_HOSPITALS = 20

try:
	HTTPX_TIMEOUT_SECONDS = float(os.getenv("HTTPX_TIMEOUT_SECONDS", "30.0"))
except (TypeError, ValueError):
	HTTPX_TIMEOUT_SECONDS = 30.0

