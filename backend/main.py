"""
BionicPRO Reports API
=====================
FastAPI-сервис, предоставляющий эндпоинт GET /reports.

Логика доступа:
1. Клиент передаёт Bearer-токен Keycloak в заголовке Authorization.
2. Сервис верифицирует подпись токена через JWKS-эндпоинт Keycloak.
3. Из токена извлекается email аутентифицированного пользователя.
4. Email сопоставляется с записью в CRM DB для получения customer_id.
5. По customer_id (= user_id в OLAP) из витрины report_mart извлекается
   агрегированный отчёт за уже обработанные Airflow периоды.
6. Пользователь получает только свои данные — доступ к чужим закрыт.
"""

import os
from typing import Any

import clickhouse_connect
import httpx
import psycopg2
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

# ─── Конфигурация ─────────────────────────────────────────────────────────────
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "reports-api")

CRM_DSN = os.getenv(
    "CRM_DSN",
    "host=crm_db port=5432 dbname=crm_db user=crm_user password=crm_password",
)

OLAP_HOST = os.getenv("OLAP_HOST", "olap_db")
OLAP_PORT = int(os.getenv("OLAP_PORT", "8123"))

JWKS_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
ISSUER = os.getenv("KEYCLOAK_ISSUER", f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}")

# ─── Приложение ───────────────────────────────────────────────────────────────
app = FastAPI(title="BionicPRO Reports API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Authorization"],
)

security = HTTPBearer()


# ─── JWKS-кэш ────────────────────────────────────────────────────────────────
_jwks_cache: dict[str, Any] | None = None


def get_jwks() -> dict:
    global _jwks_cache
    if _jwks_cache is None:
        resp = httpx.get(JWKS_URL, timeout=10)
        resp.raise_for_status()
        _jwks_cache = resp.json()
    return _jwks_cache


# ─── Зависимость: верификация токена ─────────────────────────────────────────
def get_current_user_email(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    """
    Верифицирует JWT-токен Keycloak и возвращает email пользователя.
    Если токен невалиден или отсутствует email — возвращает 401.
    """
    token = credentials.credentials
    try:
        jwks = get_jwks()
        # jose автоматически выбирает нужный ключ по kid из заголовка токена
        payload = jwt.decode(
            token,
            jwks,
            algorithms=["RS256"],
            audience=KEYCLOAK_CLIENT_ID,
            issuer=ISSUER,
            options={"verify_at_hash": False},
        )
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {exc}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    email: str | None = payload.get("email")
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token does not contain email claim",
        )
    return email


# ─── Вспомогательные функции ─────────────────────────────────────────────────
def get_customer_id_by_email(email: str) -> int | None:
    """Находит customer_id в CRM по email пользователя."""
    conn = psycopg2.connect(CRM_DSN)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM customers WHERE email = %s LIMIT 1", (email,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def query_report_mart(user_id: int) -> list[dict]:
    """
    Читает витрину report_mart для конкретного user_id.
    Возвращает только уже обработанные Airflow данные.
    Если данных нет — возвращает пустой список (период ещё не обработан).
    """
    client = clickhouse_connect.get_client(host=OLAP_HOST, port=OLAP_PORT)
    result = client.query(
        """
        SELECT
            user_id,
            user_name,
            user_email,
            prosthesis_type,
            total_signals,
            round(avg_frequency, 2)   AS avg_frequency,
            round(avg_amplitude, 4)   AS avg_amplitude,
            round(avg_duration, 2)    AS avg_duration,
            round(max_amplitude, 4)   AS max_amplitude,
            round(min_amplitude, 4)   AS min_amplitude,
            toString(first_signal_time) AS first_signal_time,
            toString(last_signal_time)  AS last_signal_time,
            toString(etl_date)          AS etl_date
        FROM report_mart
        WHERE user_id = %(uid)s
        ORDER BY etl_date DESC, prosthesis_type
        """,
        parameters={"uid": user_id},
    )

    col_names = result.column_names
    return [dict(zip(col_names, row)) for row in result.result_rows]


# ─── Эндпоинты ───────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reports")
def get_report(user_email: str = Depends(get_current_user_email)):
    """
    Возвращает отчёт о работе протеза для аутентифицированного пользователя.

    - Доступ только к собственным данным (email из токена).
    - Данные берутся из витрины report_mart, наполняемой Airflow ежедневно.
    - Если данных ещё нет (период не обработан) — возвращает пустой список
      с поясняющим сообщением.
    """
    # 1. Найти customer_id по email
    customer_id = get_customer_id_by_email(user_email)
    if customer_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No CRM record found for user '{user_email}'. "
                   "Contact support if this is unexpected.",
        )

    # 2. Запросить витрину — только уже обработанные Airflow данные
    report_rows = query_report_mart(customer_id)

    if not report_rows:
        return {
            "user_id": customer_id,
            "user_email": user_email,
            "message": (
                "No report data available yet. "
                "Data is collected daily by the ETL pipeline. "
                "Please try again after the next scheduled run (02:00 UTC)."
            ),
            "data": [],
        }

    return {
        "user_id": customer_id,
        "user_email": user_email,
        "message": "Report generated successfully.",
        "data": report_rows,
    }