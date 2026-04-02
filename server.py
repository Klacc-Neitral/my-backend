import json
import os
import sys
import time
import uuid
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import bcrypt
import jwt
from psycopg2.extras import RealDictCursor

SERVER_DIR = Path(__file__).resolve().parent
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from db import get_connection


ROOT_DIR = Path(__file__).resolve().parent.parent
CLIENT_DIR = ROOT_DIR / "docs"
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_IN_SECONDS = int(os.environ.get("JWT_EXPIRES_IN_SECONDS", "604800"))
SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        username TEXT,
        platform TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS courses (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        level TEXT,
        image TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lessons (
        id SERIAL PRIMARY KEY,
        course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        content TEXT,
        "order" INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS enrollments (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT enrollments_user_course_unique UNIQUE (user_id, course_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS progress (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
        current_lesson INTEGER,
        progress_percent INTEGER DEFAULT 0,
        completed BOOLEAN DEFAULT FALSE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT progress_user_course_unique UNIQUE (user_id, course_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quiz_questions (
        id SERIAL PRIMARY KEY,
        course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
        question TEXT NOT NULL,
        answers JSONB NOT NULL,
        correct_answer INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quiz_results (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
        score INTEGER,
        total INTEGER,
        passed BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS auth_credentials (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT auth_credentials_user_unique UNIQUE (user_id),
        CONSTRAINT auth_credentials_email_unique UNIQUE (email)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_lessons_course_id_order
        ON lessons (course_id, "order")
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_enrollments_user_id
        ON enrollments (user_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_enrollments_course_id
        ON enrollments (course_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_progress_user_id
        ON progress (user_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_progress_course_id
        ON progress (course_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_quiz_questions_course_id
        ON quiz_questions (course_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_quiz_results_user_id
        ON quiz_results (user_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_quiz_results_course_id
        ON quiz_results (course_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_credentials_email_lower_unique
        ON auth_credentials (LOWER(email))
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_auth_credentials_user_id
        ON auth_credentials (user_id)
    """,
]


def get_jwt_secret():
    secret = os.environ.get("JWT_SECRET")
    if not secret:
        raise RuntimeError("JWT_SECRET is not set")
    return secret


def normalize_email(email):
    return (email or "").strip().lower()


def ensure_schema():
    with get_connection() as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA_STATEMENTS:
                cur.execute(statement)
        conn.commit()


def get_runtime_api_base_url(headers):
    configured_url = (
        os.environ.get("PUBLIC_API_URL")
        or os.environ.get("API_BASE_URL")
        or os.environ.get("VITE_API_URL")
    )
    if configured_url:
        configured_url = configured_url.rstrip("/")
        if configured_url.endswith("/api"):
            return configured_url
        return f"{configured_url}/api"

    forwarded_proto = headers.get("X-Forwarded-Proto") or "http"
    forwarded_host = headers.get("X-Forwarded-Host") or headers.get("Host") or "127.0.0.1:8000"
    return f"{forwarded_proto}://{forwarded_host}/api"


def build_user_payload(user_row):
    name = (user_row.get("name") or "").strip()
    parts = name.split(" ", 1) if name else []
    first_name = parts[0] if parts else "Guest"
    last_name = parts[1] if len(parts) > 1 else ""

    user_id = user_row["id"]
    platform = user_row.get("platform") or detect_platform_by_user_id(user_id)

    return {
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "username": user_row.get("username"),
        "avatar_url": None,
        "platform": platform,
        "email": user_row.get("email"),
    }


def create_auth_response(user_row):
    user = build_user_payload(user_row)
    token = jwt.encode(
        {
            "sub": user["user_id"],
            "platform": user["platform"],
            "exp": int(time.time()) + JWT_EXPIRES_IN_SECONDS,
        },
        get_jwt_secret(),
        algorithm=JWT_ALGORITHM,
    )
    return {"token": token, "user": user}


def detect_platform_by_user_id(user_id):
    if user_id.startswith("tg_"):
        return "tg"
    if user_id.startswith("vk_"):
        return "vk"
    if user_id.startswith("local_"):
        return "local"
    return "guest"


def course_action(percent):
    if percent >= 100:
        return "Завершено"
    if percent > 0:
        return "Продолжить"
    return "Начать"


class AppHandler(SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/app-config.js":
            self._send_js(
                f"window.APP_CONFIG = {json.dumps({'apiBaseUrl': get_runtime_api_base_url(self.headers)}, ensure_ascii=False)};"
            )
            return

        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/auth/me":
            claims = self._require_auth()
            if not claims:
                return
            self._handle_auth_me(claims["sub"])
            return

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/courses"):
            user_id = parsed.path.split("/")[3]
            if not self._require_auth(user_id):
                return
            self._handle_get_courses(user_id)
            return

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/materials"):
            user_id = parsed.path.split("/")[3]
            if not self._require_auth(user_id):
                return
            self._handle_get_materials(user_id)
            return

        if parsed.path.startswith("/api/"):
            self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/auth/register":
            self._handle_register()
            return

        if parsed.path == "/api/auth/login":
            self._handle_login()
            return

        if parsed.path == "/api/auth/telegram":
            self._handle_auth_telegram()
            return

        if parsed.path == "/api/auth/platform":
            self._handle_auth_platform()
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_PUT(self):
        parsed = urlparse(self.path)

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/profile"):
            user_id = parsed.path.split("/")[3]
            if not self._require_auth(user_id):
                return
            self._handle_update_profile(user_id)
            return

        if parsed.path.startswith("/api/users/") and "/courses/" in parsed.path:
            parts = parsed.path.split("/")
            user_id = parts[3]
            course_id = parts[5]
            if not self._require_auth(user_id):
                return
            self._handle_update_course_state(user_id, course_id)
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_register(self):
        payload = self._read_json()
        name = (payload.get("name") or "").strip()
        email = normalize_email(payload.get("email"))
        password = payload.get("password") or ""

        if not name or not email or not password:
            self._send_json({"error": "name, email and password are required"}, status=HTTPStatus.BAD_REQUEST)
            return

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM auth_credentials
                    WHERE LOWER(email) = LOWER(%s)
                    """,
                    (email,),
                )
                if cur.fetchone():
                    self._send_json({"error": "Пользователь с таким email уже существует"}, status=HTTPStatus.CONFLICT)
                    return

                user_id = f"local_{uuid.uuid4().hex}"
                password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

                cur.execute(
                    """
                    INSERT INTO users (id, name, username, platform)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, name, username, platform, created_at
                    """,
                    (user_id, name, email, "local"),
                )

                cur.execute(
                    """
                    INSERT INTO auth_credentials (user_id, email, password_hash)
                    VALUES (%s, %s, %s)
                    """,
                    (user_id, email, password_hash),
                )

                cur.execute(
                    """
                    SELECT u.id, u.name, u.username, u.platform, u.created_at, a.email
                    FROM users u
                    LEFT JOIN auth_credentials a ON a.user_id = u.id
                    WHERE u.id = %s
                    """,
                    (user_id,),
                )
                user_row = cur.fetchone()
            conn.commit()

        self._send_json(create_auth_response(user_row), status=HTTPStatus.CREATED)

    def _handle_login(self):
        payload = self._read_json()
        email = normalize_email(payload.get("email"))
        password = payload.get("password") or ""

        if not email or not password:
            self._send_json({"error": "email and password are required"}, status=HTTPStatus.BAD_REQUEST)
            return

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        u.id,
                        u.name,
                        u.username,
                        u.platform,
                        u.created_at,
                        a.email,
                        a.password_hash
                    FROM auth_credentials a
                    JOIN users u ON u.id = a.user_id
                    WHERE LOWER(a.email) = LOWER(%s)
                    """,
                    (email,),
                )
                user_row = cur.fetchone()

        if not user_row or not bcrypt.checkpw(password.encode("utf-8"), user_row["password_hash"].encode("utf-8")):
            self._send_json({"error": "Неверный email или пароль"}, status=HTTPStatus.UNAUTHORIZED)
            return

        self._send_json(create_auth_response(user_row))

    def _handle_auth_telegram(self):
        payload = self._read_json()
        user_id = payload.get("user_id")
        first_name = (payload.get("first_name") or "").strip()
        last_name = (payload.get("last_name") or "").strip()
        username = payload.get("username")

        if not user_id:
            self._send_json({"error": "user_id is required"}, status=HTTPStatus.BAD_REQUEST)
            return

        if not str(user_id).startswith("tg_"):
            user_id = f"tg_{user_id}"

        self._send_json(self._upsert_platform_user(user_id, "tg", first_name, last_name, username))

    def _handle_auth_platform(self):
        payload = self._read_json()
        user_id = payload.get("user_id")
        platform = payload.get("platform") or detect_platform_by_user_id(str(user_id or ""))
        first_name = (payload.get("first_name") or "").strip()
        last_name = (payload.get("last_name") or "").strip()
        username = payload.get("username")

        if not user_id or not platform:
            self._send_json({"error": "user_id and platform are required"}, status=HTTPStatus.BAD_REQUEST)
            return

        self._send_json(self._upsert_platform_user(str(user_id), platform, first_name, last_name, username))

    def _upsert_platform_user(self, user_id, platform, first_name, last_name, username):
        full_name = " ".join(part for part in [first_name, last_name] if part).strip() or "Guest"

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO users (id, name, username, platform)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        username = EXCLUDED.username,
                        platform = EXCLUDED.platform
                    RETURNING id, name, username, platform, created_at
                    """,
                    (user_id, full_name, username, platform),
                )
                cur.execute(
                    """
                    SELECT u.id, u.name, u.username, u.platform, u.created_at, a.email
                    FROM users u
                    LEFT JOIN auth_credentials a ON a.user_id = u.id
                    WHERE u.id = %s
                    """,
                    (user_id,),
                )
                user_row = cur.fetchone()
            conn.commit()

        return create_auth_response(user_row)

    def _handle_update_profile(self, user_id):
        payload = self._read_json()
        first_name = (payload.get("first_name") or "").strip()
        last_name = (payload.get("last_name") or "").strip()
        username = payload.get("username")
        platform = payload.get("platform") or detect_platform_by_user_id(user_id)
        full_name = " ".join(part for part in [first_name, last_name] if part).strip() or "Guest"

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET name = %s,
                        username = %s,
                        platform = %s
                    WHERE id = %s
                    RETURNING id, name, username, platform, created_at
                    """,
                    (full_name, username, platform, user_id),
                )
                user_row = cur.fetchone()

                if user_row:
                    cur.execute(
                        """
                        SELECT u.id, u.name, u.username, u.platform, u.created_at, a.email
                        FROM users u
                        LEFT JOIN auth_credentials a ON a.user_id = u.id
                        WHERE u.id = %s
                        """,
                        (user_id,),
                    )
                    user_row = cur.fetchone()
            conn.commit()

        if not user_row:
            self._send_json({"error": "User not found"}, status=HTTPStatus.NOT_FOUND)
            return

        self._send_json(build_user_payload(user_row))

    def _handle_auth_me(self, user_id):
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT u.id, u.name, u.username, u.platform, u.created_at, a.email
                    FROM users u
                    LEFT JOIN auth_credentials a ON a.user_id = u.id
                    WHERE u.id = %s
                    """,
                    (user_id,),
                )
                user_row = cur.fetchone()

        if not user_row:
            self._send_json({"error": "User not found"}, status=HTTPStatus.NOT_FOUND)
            return

        self._send_json(build_user_payload(user_row))

    def _handle_get_courses(self, user_id):
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        c.id,
                        c.title,
                        c.description,
                        c.level,
                        c.image,
                        CASE WHEN e.id IS NULL THEN FALSE ELSE TRUE END AS is_enrolled,
                        COALESCE(p.progress_percent, 0) AS progress_percent,
                        COALESCE(p.completed, FALSE) AS completed
                    FROM courses c
                    LEFT JOIN enrollments e
                        ON e.course_id = c.id AND e.user_id = %s
                    LEFT JOIN progress p
                        ON p.course_id = c.id AND p.user_id = %s
                    ORDER BY c.id
                    """,
                    (user_id, user_id),
                )
                rows = cur.fetchall()

        courses = []
        for row in rows:
            percent = int(row["progress_percent"] or 0)
            courses.append(
                {
                    "id": row["id"],
                    "title": row["title"],
                    "desc": row["description"] or "",
                    "level": row["level"] or "",
                    "img": row["image"] or "",
                    "isEnrolled": bool(row["is_enrolled"]),
                    "percent": percent,
                    "action": course_action(percent),
                }
            )

        self._send_json(courses)

    def _handle_get_materials(self, user_id):
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        c.id AS course_id,
                        c.title AS course_name,
                        l.title AS page_title,
                        l.content,
                        l."order" AS page_number
                    FROM lessons l
                    JOIN courses c ON c.id = l.course_id
                    ORDER BY c.id, l."order"
                    """
                )
                rows = cur.fetchall()

        materials = []
        for row in rows:
            materials.append(
                {
                    "course_id": row["course_id"],
                    "course_name": row["course_name"],
                    "pageTitle": row["page_title"],
                    "text": row["content"] or "",
                    "pageNumber": row["page_number"],
                }
            )

        self._send_json(materials)

    def _handle_update_course_state(self, user_id, course_id):
        payload = self._read_json()
        is_enrolled = bool(payload.get("isEnrolled"))
        percent = int(payload.get("percent", 0))
        total_lessons = self._get_total_lessons(course_id)
        current_lesson = 0

        if total_lessons > 0 and percent > 0:
            current_lesson = max(1, round((percent / 100) * total_lessons))
            current_lesson = min(current_lesson, total_lessons)

        completed = percent >= 100

        with get_connection() as conn:
            with conn.cursor() as cur:
                if is_enrolled:
                    cur.execute(
                        """
                        INSERT INTO enrollments (user_id, course_id)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, course_id) DO NOTHING
                        """,
                        (user_id, course_id),
                    )
                    cur.execute(
                        """
                        INSERT INTO progress (user_id, course_id, current_lesson, progress_percent, completed)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, course_id) DO UPDATE
                        SET current_lesson = EXCLUDED.current_lesson,
                            progress_percent = EXCLUDED.progress_percent,
                            completed = EXCLUDED.completed,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (user_id, course_id, current_lesson, percent, completed),
                    )
                else:
                    cur.execute(
                        "DELETE FROM progress WHERE user_id = %s AND course_id = %s",
                        (user_id, course_id),
                    )
                    cur.execute(
                        "DELETE FROM enrollments WHERE user_id = %s AND course_id = %s",
                        (user_id, course_id),
                    )
            conn.commit()

        self._send_json({"ok": True})

    def _get_total_lessons(self, course_id):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT COUNT(*) FROM lessons WHERE course_id = %s',
                    (course_id,),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def _require_auth(self, expected_user_id=None):
        auth_header = self.headers.get("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            self._send_json({"error": "Missing auth token"}, status=HTTPStatus.UNAUTHORIZED)
            return None

        token = auth_header.split(" ", 1)[1].strip()
        if not token:
            self._send_json({"error": "Missing auth token"}, status=HTTPStatus.UNAUTHORIZED)
            return None

        try:
            payload = jwt.decode(token, get_jwt_secret(), algorithms=[JWT_ALGORITHM])
        except jwt.ExpiredSignatureError:
            self._send_json({"error": "Token expired"}, status=HTTPStatus.UNAUTHORIZED)
            return None
        except jwt.InvalidTokenError:
            self._send_json({"error": "Invalid auth token"}, status=HTTPStatus.UNAUTHORIZED)
            return None

        if expected_user_id and payload.get("sub") != expected_user_id:
            self._send_json({"error": "Forbidden"}, status=HTTPStatus.FORBIDDEN)
            return None

        return payload

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_js(self, source, status=HTTPStatus.OK):
        body = source.encode("utf-8")
        self.send_response(status)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")


def run():
    get_jwt_secret()
    port = int(os.environ.get("PORT", 8000))
    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        partial(AppHandler, directory=str(CLIENT_DIR)),
    )
    print(f"Server started at http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
