from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import seed
from app.db import connect
from app.routers import api
from app.services import demo_seed

app = FastAPI(title="Changeflow", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup():
    seed.init_db()
    conn = connect()
    try:
        demo_seed.seed_demo(conn)
        conn.commit()
    finally:
        conn.close()


app.include_router(api)


@app.get("/api/health")
def health():
    return {"ok": True, "project": "changeflow"}
