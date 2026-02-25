#!/usr/bin/env python3
import hmac
import json
import os
import queue
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg.rows import dict_row

try:
    from psycopg_pool import ConnectionPool, PoolTimeout
except ImportError:  # pragma: no cover - optional dependency fallback
    ConnectionPool = None
    PoolTimeout = None

ROOT_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ALLOWED_ORIGINS_RAW = os.environ.get("ALLOWED_ORIGINS", "*").strip()
LEADERBOARD_ADMIN_TOKEN = os.environ.get("LEADERBOARD_ADMIN_TOKEN", "").strip()

VALID_OUTCOMES = {"victory", "game_over", "in_progress"}
DB_CONNECT_TIMEOUT_SECONDS = 12
DB_STATEMENT_TIMEOUT_MS = int(os.environ.get("DB_STATEMENT_TIMEOUT_MS", "7000"))
DB_LOCK_RETRIES = 6
DB_RETRY_BASE_DELAY_SECONDS = 0.05
DB_POOL_MIN_SIZE = int(os.environ.get("DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_SIZE = int(os.environ.get("DB_POOL_MAX_SIZE", "5"))
DB_POOL_TIMEOUT_SECONDS = int(os.environ.get("DB_POOL_TIMEOUT_SECONDS", "10"))
DEFAULT_RESULT_LIMIT = 100
MAX_RESULT_LIMIT = 1000
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
LEADERBOARD_CACHE_TTL_SECONDS = float(os.environ.get("LEADERBOARD_CACHE_TTL_SECONDS", "3"))
SSE_QUEUE_SIZE = 128
SSE_HEARTBEAT_SECONDS = 15
IN_PROGRESS_RESULT_TTL_SECONDS = int(os.environ.get("IN_PROGRESS_RESULT_TTL_SECONDS", "900"))
STALE_IN_PROGRESS_PRUNE_MIN_INTERVAL_SECONDS = float(
    os.environ.get("STALE_IN_PROGRESS_PRUNE_MIN_INTERVAL_SECONDS", "10")
)
EXPECTED_QUESTION_COUNT = int(os.environ.get("EXPECTED_QUESTION_COUNT", "50"))
SCORE_CORRECT_DELTA = int(os.environ.get("SCORE_CORRECT_DELTA", "10"))
SCORE_INCORRECT_DELTA = int(os.environ.get("SCORE_INCORRECT_DELTA", "5"))
SCORE_STREAK_BONUS_DELTA = int(os.environ.get("SCORE_STREAK_BONUS_DELTA", "2"))
SCORE_STREAK_BONUS_INTERVAL = int(os.environ.get("SCORE_STREAK_BONUS_INTERVAL", "5"))
TRANSIENT_SQLSTATES = {
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available
    "23505",  # unique_violation (retry race on same player)
    "53300",  # too_many_connections
    "57P03",  # cannot_connect_now
    "57014",  # query_canceled (e.g. statement timeout)
    "08000",  # connection_exception
    "08001",  # sqlclient_unable_to_establish_sqlconnection
    "08003",  # connection_does_not_exist
    "08006",  # connection_failure
}

DB_POOL = None
RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_BUCKETS = {}
RESULTS_CACHE_LOCK = threading.Lock()
RESULTS_CACHE = {}
STALE_IN_PROGRESS_PRUNE_LOCK = threading.Lock()
STALE_IN_PROGRESS_LAST_PRUNE_AT = 0.0


def parse_allowed_origins(raw: str):
    if raw == "*" or not raw:
        return None
    parsed = {origin.strip() for origin in raw.split(",") if origin.strip()}
    return parsed or None


ALLOWED_ORIGINS = parse_allowed_origins(ALLOWED_ORIGINS_RAW)


def init_db() -> None:
    def _initialize():
        with open_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass('public.game_results') IS NOT NULL AS exists"
                )
                game_results_exists = bool(cur.fetchone()["exists"])

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS game_results (
                        id BIGSERIAL PRIMARY KEY,
                        player_name TEXT NOT NULL,
                        player_id TEXT NOT NULL,
                        player_name_normalized TEXT NOT NULL,
                        score INTEGER NOT NULL CHECK (score >= 0),
                        correct_count INTEGER NOT NULL CHECK (correct_count >= 0),
                        wrong_count INTEGER NOT NULL DEFAULT 0 CHECK (wrong_count >= 0),
                        timeout_count INTEGER NOT NULL DEFAULT 0 CHECK (timeout_count >= 0),
                        total_answered INTEGER NOT NULL CHECK (total_answered >= 0),
                        accuracy INTEGER NOT NULL CHECK (accuracy >= 0 AND accuracy <= 100),
                        duration_ms INTEGER NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
                        outcome TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS result_submissions (
                        id BIGSERIAL PRIMARY KEY,
                        player_id TEXT NOT NULL,
                        attempt_id TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        UNIQUE (player_id, attempt_id)
                    )
                    """
                )
                if game_results_exists:
                    # Compatibility migrations for older deployments that predate
                    # the current schema defined in CREATE TABLE above.
                    for migration_sql in (
                        """
                        ALTER TABLE game_results
                        ADD COLUMN IF NOT EXISTS player_name_normalized TEXT
                        """,
                        """
                        ALTER TABLE game_results
                        ADD COLUMN IF NOT EXISTS player_id TEXT
                        """,
                        """
                        ALTER TABLE game_results
                        ADD COLUMN IF NOT EXISTS wrong_count INTEGER NOT NULL DEFAULT 0
                        """,
                        """
                        ALTER TABLE game_results
                        ADD COLUMN IF NOT EXISTS timeout_count INTEGER NOT NULL DEFAULT 0
                        """,
                        """
                        ALTER TABLE game_results
                        ADD COLUMN IF NOT EXISTS duration_ms INTEGER NOT NULL DEFAULT 0
                        """,
                    ):
                        cur.execute(migration_sql)
                cur.execute(
                    """
                    UPDATE game_results
                    SET player_name_normalized = LOWER(BTRIM(player_name))
                    WHERE player_name_normalized IS NULL OR player_name_normalized = ''
                    """
                )
                cur.execute(
                    """
                    UPDATE game_results
                    SET player_id = CONCAT('legacy-', id::text)
                    WHERE player_id IS NULL OR player_id = ''
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    ALTER COLUMN player_name_normalized SET NOT NULL
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    ALTER COLUMN player_id SET NOT NULL
                    """
                )
                cur.execute(
                    """
                    DROP INDEX IF EXISTS idx_game_results_player_name_unique
                    """
                )
                cur.execute(
                    """
                    DROP INDEX IF EXISTS idx_game_results_player_email_unique
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    DROP COLUMN IF EXISTS player_email_normalized
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    DROP COLUMN IF EXISTS player_email
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    DROP CONSTRAINT IF EXISTS game_results_outcome_check
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE game_results
                    ADD CONSTRAINT game_results_outcome_check
                    CHECK (outcome IN ('victory', 'game_over', 'in_progress'))
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_game_results_created_at
                    ON game_results(created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_game_results_rank
                    ON game_results(score DESC, accuracy DESC, created_at ASC, id ASC)
                    """
                )
                # Keep only one leaderboard row per player id.
                cur.execute(
                    """
                    DELETE FROM game_results target
                    USING (
                        SELECT id
                        FROM (
                            SELECT
                                id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY player_id
                                    ORDER BY score DESC, accuracy DESC, created_at ASC, id ASC
                                ) AS row_num
                            FROM game_results
                        ) ranked
                        WHERE row_num > 1
                    ) duplicate_rows
                    WHERE target.id = duplicate_rows.id
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_game_results_player_id_unique
                    ON game_results(player_id)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_result_submissions_player_attempt
                    ON result_submissions(player_id, attempt_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_result_submissions_created_at
                    ON result_submissions(created_at DESC)
                    """
                )
                cur.execute(
                    """
                    DELETE FROM result_submissions
                    WHERE created_at < NOW() - INTERVAL '30 days'
                    """
                )
            conn.commit()

    with_db_retry(_initialize)


def is_transient_db_error(exc: Exception) -> bool:
    if PoolTimeout is not None and isinstance(exc, PoolTimeout):
        return True

    if isinstance(exc, psycopg.OperationalError):
        return True

    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate in TRANSIENT_SQLSTATES:
        return True

    return False


def with_db_retry(operation):
    delay = DB_RETRY_BASE_DELAY_SECONDS

    for attempt in range(DB_LOCK_RETRIES):
        try:
            return operation()
        except Exception as exc:
            if not is_transient_db_error(exc) or attempt == DB_LOCK_RETRIES - 1:
                raise
            time.sleep(delay)
            delay *= 2


def init_db_pool() -> None:
    global DB_POOL
    if ConnectionPool is None:
        DB_POOL = None
        return
    if DB_POOL is not None:
        return
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure a PostgreSQL connection string."
        )

    DB_POOL = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=max(1, DB_POOL_MIN_SIZE),
        max_size=max(DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE),
        timeout=max(1, DB_POOL_TIMEOUT_SECONDS),
        kwargs={
            "row_factory": dict_row,
            "connect_timeout": DB_CONNECT_TIMEOUT_SECONDS,
        },
    )


def close_db_pool() -> None:
    global DB_POOL
    if DB_POOL is not None:
        DB_POOL.close()
        DB_POOL = None


def configure_connection(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT_MS}ms'")


@contextmanager
def open_db_connection():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure a PostgreSQL connection string."
        )

    if DB_POOL is not None:
        with DB_POOL.connection() as conn:
            configure_connection(conn)
            yield conn
        return

    conn = psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
    )
    try:
        configure_connection(conn)
        yield conn
    finally:
        conn.close()


def normalize_token(value: str) -> str:
    value = value.strip().lower()
    allowed = []
    for ch in value:
        if ch.isalnum() or ch == "-":
            allowed.append(ch)
    return "".join(allowed)


def validate_id_token(value: str, field: str, min_length: int = 8, max_length: int = 80) -> str:
    normalized = normalize_token(value)
    if len(normalized) < min_length or len(normalized) > max_length:
        raise ValueError(f"{field} is invalid")
    return normalized


def parse_optional_int_field(payload: dict, field: str, default: int = 0, min_value: int = 0) -> int:
    value = payload.get(field, default)
    if value is None or value == "":
        number = default
    elif isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    elif isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.strip().isdigit():
        number = int(value.strip())
    else:
        raise ValueError(f"{field} must be a number")

    if number < min_value:
        raise ValueError(f"{field} must be >= {min_value}")
    return number


def invalidate_results_cache() -> None:
    with RESULTS_CACHE_LOCK:
        RESULTS_CACHE.clear()


def get_cached_results(cache_key: str):
    now = time.time()
    with RESULTS_CACHE_LOCK:
        cached = RESULTS_CACHE.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            RESULTS_CACHE.pop(cache_key, None)
            return None
        return payload


def set_cached_results(cache_key: str, payload: dict) -> None:
    ttl = max(0.1, LEADERBOARD_CACHE_TTL_SECONDS)
    expires_at = time.time() + ttl
    with RESULTS_CACHE_LOCK:
        RESULTS_CACHE[cache_key] = (expires_at, payload)


def should_prune_stale_in_progress_results() -> bool:
    if IN_PROGRESS_RESULT_TTL_SECONDS <= 0:
        return False

    min_interval = max(0.0, STALE_IN_PROGRESS_PRUNE_MIN_INTERVAL_SECONDS)
    now = time.monotonic()
    global STALE_IN_PROGRESS_LAST_PRUNE_AT
    with STALE_IN_PROGRESS_PRUNE_LOCK:
        if min_interval > 0 and (now - STALE_IN_PROGRESS_LAST_PRUNE_AT) < min_interval:
            return False
        STALE_IN_PROGRESS_LAST_PRUNE_AT = now
    return True


def prune_stale_in_progress_results() -> list[int]:
    if IN_PROGRESS_RESULT_TTL_SECONDS <= 0:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=IN_PROGRESS_RESULT_TTL_SECONDS)

    def _prune():
        with open_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM game_results
                    WHERE outcome = 'in_progress' AND created_at < %s
                    RETURNING id
                    """,
                    (cutoff,),
                )
                rows = cur.fetchall() or []
            conn.commit()
            return [int(row["id"]) for row in rows if row and row.get("id") is not None]

    return with_db_retry(_prune)


def allow_rate_limit(key: str, max_tokens: int, refill_per_second: float) -> bool:
    now = time.monotonic()
    with RATE_LIMIT_LOCK:
        tokens, last_seen = RATE_LIMIT_BUCKETS.get(key, (float(max_tokens), now))
        tokens = min(float(max_tokens), tokens + (now - last_seen) * refill_per_second)
        if tokens < 1:
            RATE_LIMIT_BUCKETS[key] = (tokens, now)
            return False

        RATE_LIMIT_BUCKETS[key] = (tokens - 1, now)

        # Keep memory bounded for long-running processes.
        if len(RATE_LIMIT_BUCKETS) > 50000:
            stale_before = now - 60
            stale_keys = [
                bucket_key
                for bucket_key, (_, timestamp) in RATE_LIMIT_BUCKETS.items()
                if timestamp < stale_before
            ]
            for bucket_key in stale_keys[:5000]:
                RATE_LIMIT_BUCKETS.pop(bucket_key, None)
    return True


def validate_score_payload(
    *,
    score: int,
    correct_count: int,
    wrong_count: int,
    timeout_count: int,
    total_answered: int,
    duration_ms: int,
    outcome: str,
) -> None:
    if total_answered > EXPECTED_QUESTION_COUNT:
        raise ValueError("totalAnswered exceeds expected question count")

    if correct_count > total_answered:
        raise ValueError("correctCount cannot be greater than totalAnswered")

    if wrong_count + timeout_count + correct_count != total_answered:
        raise ValueError("Answer counts do not add up")

    if score < 0:
        raise ValueError("score must be >= 0")

    # Frontend awards +2 every 5 consecutive correct answers.
    # We only have aggregate counts here, so allow the maximum possible streak bonus
    # (all correct answers grouped into one continuous streak).
    max_streak_bonus = 0
    if SCORE_STREAK_BONUS_INTERVAL > 0 and SCORE_STREAK_BONUS_DELTA > 0:
        max_streak_bonus = (correct_count // SCORE_STREAK_BONUS_INTERVAL) * SCORE_STREAK_BONUS_DELTA

    if score > (correct_count * SCORE_CORRECT_DELTA) + max_streak_bonus:
        raise ValueError("score exceeds maximum for the provided counts")

    if outcome == "victory" and total_answered != EXPECTED_QUESTION_COUNT:
        raise ValueError("victory requires all questions to be answered")

    if duration_ms > 0:
        if duration_ms > 4 * 60 * 60 * 1000:
            raise ValueError("durationMs is too large")


def serialize_created_at(value) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")

    if isinstance(value, str) and value:
        return value

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def serialize_result_row(row: dict) -> dict:
    return {
        "id": int(row["id"]),
        "playerName": row["playerName"],
        "score": int(row["score"]),
        "correctCount": int(row["correctCount"]),
        "wrongCount": int(row.get("wrongCount", 0)),
        "timeoutCount": int(row.get("timeoutCount", 0)),
        "totalAnswered": int(row["totalAnswered"]),
        "accuracy": int(row["accuracy"]),
        "durationMs": int(row.get("durationMs", 0)),
        "outcome": row["outcome"],
        "createdAt": serialize_created_at(row.get("createdAt")),
    }


def read_json_body(handler: SimpleHTTPRequestHandler) -> dict:
    raw_length = handler.headers.get("Content-Length")
    if raw_length is None:
        raise ValueError("Missing Content-Length header")

    length = int(raw_length)
    body = handler.rfile.read(length)
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON payload") from exc

    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")

    return payload


def parse_int_field(payload: dict, field: str, min_value: int = 0) -> int:
    value = payload.get(field)
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.strip().isdigit():
        number = int(value.strip())
    else:
        raise ValueError(f"{field} must be a number")

    if number < min_value:
        raise ValueError(f"{field} must be >= {min_value}")
    return number


def normalize_player_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


class QuizHandler(SimpleHTTPRequestHandler):
    subscribers = set()
    subscribers_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def resolve_allow_origin(self) -> str:
        if ALLOWED_ORIGINS is None:
            return "*"
        request_origin = (self.headers.get("Origin") or "").strip()
        if request_origin and request_origin in ALLOWED_ORIGINS:
            return request_origin
        return ""

    def end_headers(self):
        allow_origin = self.resolve_allow_origin()
        if allow_origin:
            self.send_header("Access-Control-Allow-Origin", allow_origin)
            if allow_origin != "*":
                self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Game-Version, X-Admin-Token")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def get_client_ip(self) -> str:
        forwarded_for = (self.headers.get("X-Forwarded-For") or "").strip()
        if forwarded_for:
            first = forwarded_for.split(",")[0].strip()
            if first:
                return first
        return self.client_address[0] if self.client_address else "unknown"

    def is_rate_limited(self, bucket_key: str, max_tokens: int, refill_per_second: float) -> bool:
        return not allow_rate_limit(bucket_key, max_tokens=max_tokens, refill_per_second=refill_per_second)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self.send_response(302)
            self.send_header("Location", "/Zoho-Logo2.html")
            self.end_headers()
            return
        if parsed.path == "/healthz":
            self.handle_health_check()
            return
        if parsed.path == "/api/results/stream":
            self.handle_results_stream()
            return
        if parsed.path == "/api/results":
            self.handle_list_results(parsed)
            return
        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/results":
            self.handle_create_result()
            return
        self.send_json(404, {"error": "Not found"})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/results":
            self.handle_clear_results()
            return
        self.send_json(404, {"error": "Not found"})

    @classmethod
    def add_subscriber(cls, subscriber_queue):
        with cls.subscribers_lock:
            cls.subscribers.add(subscriber_queue)

    @classmethod
    def remove_subscriber(cls, subscriber_queue):
        with cls.subscribers_lock:
            cls.subscribers.discard(subscriber_queue)

    @classmethod
    def broadcast_event(cls, event_name: str, payload: dict):
        message = f"event: {event_name}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
        with cls.subscribers_lock:
            subscribers = list(cls.subscribers)

        for subscriber_queue in subscribers:
            try:
                subscriber_queue.put_nowait(message)
            except queue.Full:
                try:
                    subscriber_queue.get_nowait()
                except queue.Empty:
                    pass
                try:
                    subscriber_queue.put_nowait(message)
                except queue.Full:
                    # If a client is too slow, skip this event for that client.
                    pass

    def handle_results_stream(self):
        client_ip = self.get_client_ip()
        if self.is_rate_limited(f"sse:{client_ip}", max_tokens=5, refill_per_second=0.08):
            self.send_json(429, {"error": "Too many stream connections"})
            return

        subscriber_queue = queue.Queue(maxsize=SSE_QUEUE_SIZE)
        self.add_subscriber(subscriber_queue)

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        try:
            self.wfile.write(b"retry: 2000\n\n")
            self.wfile.flush()

            while True:
                try:
                    message = subscriber_queue.get(timeout=SSE_HEARTBEAT_SECONDS)
                except queue.Empty:
                    message = ": ping\n\n"

                self.wfile.write(message.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.remove_subscriber(subscriber_queue)

    def handle_health_check(self):
        try:
            def ping_database():
                with open_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 AS ok")
                        row = cur.fetchone()
                        return bool(row and int(row["ok"]) == 1)

            database_ok = with_db_retry(ping_database)
            if not database_ok:
                self.send_json(503, {"status": "degraded", "database": "down"})
                return

            self.send_json(200, {"status": "ok", "database": "up"})
        except Exception:
            self.send_json(503, {"status": "degraded", "database": "down"})

    def handle_create_result(self):
        client_ip = self.get_client_ip()
        if self.is_rate_limited(f"post-ip:{client_ip}", max_tokens=120, refill_per_second=2.0):
            self.send_json(429, {"error": "Too many submissions from this IP. Please retry shortly."})
            return

        try:
            payload = read_json_body(self)

            player_name = str(payload.get("playerName", "")).strip()
            player_id = validate_id_token(str(payload.get("playerId", "")), "playerId")
            attempt_raw = str(payload.get("attemptId", "") or payload.get("submissionId", "")).strip()
            attempt_id = (
                validate_id_token(attempt_raw, "attemptId", min_length=6, max_length=120)
                if attempt_raw
                else normalize_token(
                    f"{player_id}-{payload.get('score', '')}-{payload.get('correctCount', '')}-"
                    f"{payload.get('wrongCount', '')}-{payload.get('timeoutCount', '')}-"
                    f"{payload.get('totalAnswered', '')}-{payload.get('outcome', '')}"
                )[:120]
            )
            player_name_normalized = normalize_player_name(player_name)
            score = parse_int_field(payload, "score", min_value=0)
            correct_count = parse_int_field(payload, "correctCount", min_value=0)
            total_answered = parse_int_field(payload, "totalAnswered", min_value=0)
            timeout_count = parse_optional_int_field(payload, "timeoutCount", default=0, min_value=0)
            wrong_count = parse_optional_int_field(
                payload,
                "wrongCount",
                default=max(0, total_answered - correct_count - timeout_count),
                min_value=0,
            )
            duration_ms = parse_optional_int_field(payload, "durationMs", default=0, min_value=0)
            outcome = str(payload.get("outcome", "")).strip()
            update_mode_raw = str(payload.get("updateMode", "best")).strip().lower()
            update_mode = "replace" if update_mode_raw == "live" else update_mode_raw

            if len(player_name) < 2:
                raise ValueError("playerName must be at least 2 characters")
            if outcome not in VALID_OUTCOMES:
                raise ValueError("outcome must be victory, game_over, or in_progress")
            if update_mode not in {"best", "replace"}:
                raise ValueError("updateMode must be best or replace")
            validate_score_payload(
                score=score,
                correct_count=correct_count,
                wrong_count=wrong_count,
                timeout_count=timeout_count,
                total_answered=total_answered,
                duration_ms=duration_ms,
                outcome=outcome,
            )

            if self.is_rate_limited(f"post-player:{player_id}", max_tokens=80, refill_per_second=1.0):
                self.send_json(429, {"error": "Too many submissions for this player. Please retry shortly."})
                return

            accuracy = (
                round((correct_count / total_answered) * 100)
                if total_answered > 0
                else 0
            )
            created_at = datetime.now(timezone.utc)

            def save_result():
                with open_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT
                                id,
                                player_name AS "playerName",
                                score,
                                correct_count AS "correctCount",
                                wrong_count AS "wrongCount",
                                timeout_count AS "timeoutCount",
                                total_answered AS "totalAnswered",
                                accuracy,
                                duration_ms AS "durationMs",
                                outcome,
                                created_at AS "createdAt"
                            FROM game_results
                            WHERE player_id = %s
                            ORDER BY score DESC, accuracy DESC, created_at ASC, id ASC
                            LIMIT 1
                            FOR UPDATE
                            """,
                            (player_id,),
                        )
                        existing = cur.fetchone()

                        if update_mode != "replace":
                            cur.execute(
                                """
                                INSERT INTO result_submissions (player_id, attempt_id, created_at)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (player_id, attempt_id) DO NOTHING
                                RETURNING id
                                """,
                                (player_id, attempt_id, created_at),
                            )
                            submission_inserted = cur.fetchone() is not None
                            if not submission_inserted and existing is not None:
                                conn.commit()
                                return (200, "unchanged", serialize_result_row(existing), False)

                        if existing is None:
                            cur.execute(
                                """
                                INSERT INTO game_results (
                                    player_name,
                                    player_id,
                                    player_name_normalized,
                                    score,
                                    correct_count,
                                    wrong_count,
                                    timeout_count,
                                    total_answered,
                                    accuracy,
                                    duration_ms,
                                    outcome,
                                    created_at
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING
                                    id,
                                    player_name AS "playerName",
                                    score,
                                    correct_count AS "correctCount",
                                    wrong_count AS "wrongCount",
                                    timeout_count AS "timeoutCount",
                                    total_answered AS "totalAnswered",
                                    accuracy,
                                    duration_ms AS "durationMs",
                                    outcome,
                                    created_at AS "createdAt"
                                """,
                                (
                                    player_name,
                                    player_id,
                                    player_name_normalized,
                                    score,
                                    correct_count,
                                    wrong_count,
                                    timeout_count,
                                    total_answered,
                                    accuracy,
                                    duration_ms,
                                    outcome,
                                    created_at,
                                ),
                            )
                            created = cur.fetchone()
                            conn.commit()
                            return (201, "result_created", serialize_result_row(created), True)

                        should_replace_existing = update_mode == "replace"
                        existing_score = int(existing["score"])
                        existing_accuracy = int(existing["accuracy"])
                        is_better = score > existing_score or (
                            score == existing_score and accuracy > existing_accuracy
                        )

                        if should_replace_existing:
                            is_same_snapshot = (
                                str(existing["playerName"]) == player_name and
                                int(existing["score"]) == score and
                                int(existing["correctCount"]) == correct_count and
                                int(existing["wrongCount"]) == wrong_count and
                                int(existing["timeoutCount"]) == timeout_count and
                                int(existing["totalAnswered"]) == total_answered and
                                int(existing["accuracy"]) == accuracy and
                                int(existing["durationMs"]) == duration_ms and
                                str(existing["outcome"]) == outcome
                            )
                            if is_same_snapshot:
                                conn.commit()
                                return (200, "unchanged", serialize_result_row(existing), False)

                        if should_replace_existing or is_better:
                            result_id = int(existing["id"])
                            cur.execute(
                                """
                                UPDATE game_results
                                SET
                                    player_name = %s,
                                    player_id = %s,
                                    player_name_normalized = %s,
                                    score = %s,
                                    correct_count = %s,
                                    wrong_count = %s,
                                    timeout_count = %s,
                                    total_answered = %s,
                                    accuracy = %s,
                                    duration_ms = %s,
                                    outcome = %s,
                                    created_at = %s
                                WHERE id = %s
                                RETURNING
                                    id,
                                    player_name AS "playerName",
                                    score,
                                    correct_count AS "correctCount",
                                    wrong_count AS "wrongCount",
                                    timeout_count AS "timeoutCount",
                                    total_answered AS "totalAnswered",
                                    accuracy,
                                    duration_ms AS "durationMs",
                                    outcome,
                                    created_at AS "createdAt"
                                """,
                                (
                                    player_name,
                                    player_id,
                                    player_name_normalized,
                                    score,
                                    correct_count,
                                    wrong_count,
                                    timeout_count,
                                    total_answered,
                                    accuracy,
                                    duration_ms,
                                    outcome,
                                    created_at,
                                    result_id,
                                ),
                            )
                            updated = cur.fetchone()
                            conn.commit()
                            return (200, "result_updated", serialize_result_row(updated), True)

                        conn.commit()
                        return (200, "unchanged", serialize_result_row(existing), False)

            status_code, event_name, result, did_change = with_db_retry(save_result)
            if did_change:
                invalidate_results_cache()
            self.send_json(
                status_code,
                {
                    "status": "accepted",
                    "idempotent": event_name == "unchanged",
                    "result": result,
                },
            )
            if event_name in {"result_created", "result_updated"}:
                self.broadcast_event(event_name, {"result": result})
            if should_prune_stale_in_progress_results():
                try:
                    pruned_ids = prune_stale_in_progress_results()
                    if pruned_ids:
                        invalidate_results_cache()
                        self.broadcast_event("results_removed", {"ids": pruned_ids})
                except Exception:
                    # Stale-row cleanup is best-effort and should not fail score submission.
                    pass
        except ValueError as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception as exc:
            if isinstance(exc, RuntimeError) or is_transient_db_error(exc):
                self.send_json(503, {"error": "Database is busy. Please retry."})
                return
            self.send_json(500, {"error": "Internal server error"})

    def handle_list_results(self, parsed):
        client_ip = self.get_client_ip()
        if self.is_rate_limited(f"get:{client_ip}", max_tokens=120, refill_per_second=2):
            self.send_json(429, {"error": "Too many leaderboard requests. Please retry shortly."})
            return

        if should_prune_stale_in_progress_results():
            try:
                pruned_ids = prune_stale_in_progress_results()
                if pruned_ids:
                    invalidate_results_cache()
                    self.broadcast_event("results_removed", {"ids": pruned_ids})
            except Exception:
                # Cleanup is best-effort; serve leaderboard even if prune fails.
                pass

        query = parse_qs(parsed.query)
        sort_mode = query.get("sort", ["latest"])[0].strip().lower()
        use_pagination = "page" in query or "pageSize" in query
        page = 1
        page_size = DEFAULT_PAGE_SIZE
        offset = 0
        limit = DEFAULT_RESULT_LIMIT

        if use_pagination:
            try:
                page = max(1, int(query.get("page", ["1"])[0]))
            except ValueError:
                page = 1
            try:
                page_size = max(1, min(MAX_PAGE_SIZE, int(query.get("pageSize", [str(DEFAULT_PAGE_SIZE)])[0])))
            except ValueError:
                page_size = DEFAULT_PAGE_SIZE
            limit = page_size
            offset = (page - 1) * page_size
        else:
            raw_limit = query.get("limit", [str(DEFAULT_RESULT_LIMIT)])[0].strip().lower()
            if raw_limit == "all":
                limit = None
            else:
                try:
                    limit = max(1, min(MAX_RESULT_LIMIT, int(raw_limit)))
                except ValueError:
                    limit = DEFAULT_RESULT_LIMIT

        order_clause = (
            "score DESC, accuracy DESC, created_at ASC, id ASC"
            if sort_mode == "leaderboard"
            else "id DESC"
        )
        stale_in_progress_cutoff = None
        if IN_PROGRESS_RESULT_TTL_SECONDS > 0:
            stale_in_progress_cutoff = (
                datetime.now(timezone.utc) - timedelta(seconds=IN_PROGRESS_RESULT_TTL_SECONDS)
            )
        cache_key = None
        if sort_mode == "leaderboard" and limit is not None and offset == 0:
            cache_key = f"leaderboard:v1:limit:{limit}"
            cached_payload = get_cached_results(cache_key)
            if cached_payload is not None:
                self.send_json(
                    200,
                    cached_payload,
                    headers={"Cache-Control": f"public, max-age={int(max(1, LEADERBOARD_CACHE_TTL_SECONDS))}"},
                )
                return

        def fetch_results():
            with open_db_connection() as conn:
                with conn.cursor() as cur:
                    where_clauses = []
                    params = []
                    if stale_in_progress_cutoff is not None:
                        where_clauses.append("NOT (outcome = 'in_progress' AND created_at < %s)")
                        params.append(stale_in_progress_cutoff)

                    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                    base_query = f"""
                        SELECT
                            id,
                            player_name AS "playerName",
                            score,
                            correct_count AS "correctCount",
                            wrong_count AS "wrongCount",
                            timeout_count AS "timeoutCount",
                            total_answered AS "totalAnswered",
                            accuracy,
                            duration_ms AS "durationMs",
                            outcome,
                            created_at AS "createdAt"
                        FROM game_results
                        {where_sql}
                        ORDER BY {order_clause}
                    """
                    if limit is None:
                        cur.execute(base_query, tuple(params))
                    else:
                        cur.execute(f"{base_query} LIMIT %s OFFSET %s", tuple(params) + (limit, offset))
                    return cur.fetchall()

        try:
            rows = with_db_retry(fetch_results)
        except Exception as exc:
            if isinstance(exc, RuntimeError) or is_transient_db_error(exc):
                self.send_json(503, {"error": "Database is busy. Please retry."})
                return
            self.send_json(500, {"error": "Internal server error"})
            return

        payload = {
            "results": [serialize_result_row(row) for row in rows],
            "sort": sort_mode,
            "generatedAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        if use_pagination:
            payload["page"] = page
            payload["pageSize"] = page_size

        if cache_key is not None:
            set_cached_results(cache_key, payload)
            self.send_json(
                200,
                payload,
                headers={"Cache-Control": f"public, max-age={int(max(1, LEADERBOARD_CACHE_TTL_SECONDS))}"},
            )
            return

        self.send_json(200, payload, headers={"Cache-Control": "no-store"})

    def handle_clear_results(self):
        try:
            configured_admin_token = LEADERBOARD_ADMIN_TOKEN
            if not configured_admin_token:
                self.send_json(503, {"error": "Leaderboard admin token is not configured."})
                return

            provided_admin_token = (self.headers.get("X-Admin-Token") or "").strip()
            if not provided_admin_token or not hmac.compare_digest(provided_admin_token, configured_admin_token):
                self.send_json(403, {"error": "Forbidden"})
                return

            client_ip = self.get_client_ip()
            if self.is_rate_limited(f"delete:{client_ip}", max_tokens=2, refill_per_second=1 / 30):
                self.send_json(429, {"error": "Too many clear requests. Please retry shortly."})
                return

            def clear_results():
                with open_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM game_results")
                        deleted_results = cur.rowcount
                        cur.execute("DELETE FROM result_submissions")
                        deleted_submissions = cur.rowcount
                    conn.commit()
                    return deleted_results, deleted_submissions

            deleted_count, deleted_submissions = with_db_retry(clear_results)
            invalidate_results_cache()

            self.send_json(
                200,
                {
                    "deleted": deleted_count,
                    "deletedSubmissions": deleted_submissions,
                    "message": "All leaderboard records were cleared.",
                },
            )
            self.broadcast_event("results_cleared", {"deleted": deleted_count})
        except Exception as exc:
            if isinstance(exc, RuntimeError) or is_transient_db_error(exc):
                self.send_json(503, {"error": "Database is busy. Please retry."})
                return
            self.send_json(500, {"error": "Internal server error"})

    def send_json(self, status_code: int, payload: dict, headers: Optional[dict] = None):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if headers:
            for header_name, header_value in headers.items():
                self.send_header(header_name, str(header_value))
        self.end_headers()
        self.wfile.write(body)


class HighCapacityHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


def run_server():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is required. Set it to a PostgreSQL connection string."
        )

    init_db_pool()
    init_db()
    host = os.environ.get("HOST", "0.0.0.0")
    raw_port = os.environ.get("PORT", "8000")
    try:
        port = int(raw_port)
    except ValueError:
        port = 8000

    server = HighCapacityHTTPServer((host, port), QuizHandler)
    print(f"Server running at: http://localhost:{port}")
    print("Database: PostgreSQL (DATABASE_URL configured)")
    if ConnectionPool is None:
        print("DB Pool: disabled (install psycopg-pool for connection pooling)")
    else:
        print(f"DB Pool: enabled ({DB_POOL_MIN_SIZE}..{DB_POOL_MAX_SIZE})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
    finally:
        server.server_close()
        close_db_pool()


if __name__ == "__main__":
    run_server()
