import json
import sys
import os
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from psycopg2.extras import RealDictCursor

SERVER_DIR = Path(__file__).resolve().parent
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from db import get_connection


def build_user_payload(user_row):
    name = (user_row.get("name") or "").strip()
    parts = name.split(" ", 1) if name else []
    first_name = parts[0] if parts else "Guest"
    last_name = parts[1] if len(parts) > 1 else ""

    user_id = user_row["id"]
    if user_id.startswith("tg_"):
        platform = "tg"
    elif user_id.startswith("vk_"):
        platform = "vk"
    else:
        platform = user_row.get("platform") or "guest"

    return {
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "username": user_row.get("username"),
        "avatar_url": None,
        "platform": platform,
    }


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

        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/courses"):
            user_id = parsed.path.split("/")[3]
            self._handle_get_courses(user_id)
            return

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/materials"):
            user_id = parsed.path.split("/")[3]
            self._handle_get_materials(user_id)
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/auth/platform":
            self._handle_auth_platform()
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_PUT(self):
        parsed = urlparse(self.path)

        if parsed.path.startswith("/api/users/") and parsed.path.endswith("/profile"):
            user_id = parsed.path.split("/")[3]
            self._handle_update_profile(user_id)
            return

        if parsed.path.startswith("/api/users/") and "/courses/" in parsed.path:
            parts = parsed.path.split("/")
            user_id = parts[3]
            course_id = parts[5]
            self._handle_update_course_state(user_id, course_id)
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_auth_platform(self):
        payload = self._read_json()
        user_id = payload.get("user_id")
        platform = payload.get("platform")
        first_name = (payload.get("first_name") or "").strip()
        last_name = (payload.get("last_name") or "").strip()
        username = payload.get("username")

        if not user_id or not platform:
            self._send_json({"error": "user_id and platform are required"}, status=HTTPStatus.BAD_REQUEST)
            return

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
                user_row = cur.fetchone()
            conn.commit()

        self._send_json(build_user_payload(user_row))

    def _handle_update_profile(self, user_id):
        payload = self._read_json()
        first_name = (payload.get("first_name") or "").strip()
        last_name = (payload.get("last_name") or "").strip()
        username = payload.get("username")
        platform = payload.get("platform") or self._detect_platform_by_user_id(user_id)
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
            conn.commit()

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

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _detect_platform_by_user_id(self, user_id):
        if user_id.startswith("tg_"):
            return "tg"
        if user_id.startswith("vk_"):
            return "vk"
        return "guest"


def run():
    port = int(os.environ.get("PORT", 8000))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    print(f"Server started at http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()