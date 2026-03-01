import os
import base64
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
import mysql.connector

app = FastAPI()

class FormPayload(BaseModel):
    nombre: str = Field(min_length=1, max_length=100)
    apellido: str = Field(min_length=1, max_length=100)
    submission_id: str | None = Field(default=None, max_length=80)

def ssl_ca_path():
    # Opcional: si usas CA en base64 (Aiven suele dar CA)
    b64 = os.getenv("MYSQL_SSL_CA_B64")
    if not b64:
        return None
    path = "/tmp/aiven-ca.pem"
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(base64.b64decode(b64))
    return path

def get_conn():
    ssl_ca = ssl_ca_path()
    kwargs = dict(
        host=os.environ["MYSQL_HOST"],
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DB"],
    )
    if ssl_ca:
        kwargs["ssl_ca"] = ssl_ca
    return mysql.connector.connect(**kwargs)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/forms/submission")
def ingest(payload: FormPayload, x_api_key: str | None = Header(default=None)):
    if x_api_key != os.environ.get("INGEST_API_KEY"):
        raise HTTPException(status_code=401, detail="Invalid API key")

    cn = get_conn()
    cur = cn.cursor()

    if payload.submission_id:
        cur.execute(
            "INSERT INTO persona (nombre, apellido) VALUES (%s, %s)",
            (payload.nombre.strip(), payload.apellido.strip()),
        )
    else:
        cur.execute(
            "INSERT INTO persona (nombre, apellido) VALUES (%s, %s)",
            (payload.nombre.strip(), payload.apellido.strip()),
        )

    cn.commit()
    cur.close()
    cn.close()
    return {"ok": True}

def get_conn_1():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")),
    )

def db_fetch_latest(limit: int = 1000):
    conn = get_conn_1()
    cur = conn.cursor()
    try:
        cur = conn.cursor(dictionary=True)
        # Cambia `submissions` por tu tabla real
        cur.execute(
            """
            SELECT *
            FROM submissions
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()  # list[dict]
        return rows
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

@app.get("/forms/submision")
def list_submissions(limit: int = Query(1000, ge=1, le=20000)):
    return db_fetch_latest(limit)
