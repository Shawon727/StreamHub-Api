# StreamHub entry — Vercel / local
# Do not import heavy optional modules here; api.py handles them safely.
from api import app

# Vercel Python runtime looks for `app` (ASGI)
__all__ = ["app"]

if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
