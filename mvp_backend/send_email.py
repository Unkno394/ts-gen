import json
import os
import random
import re
import smtplib
import ssl
import time
from email.message import EmailMessage
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib import error as urlerror
from urllib.parse import urlparse
from urllib.request import Request, urlopen

def load_dotenv(path: Path):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv(Path(__file__).parent / ".env")

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
CODE_TTL_SECONDS = int(os.getenv("CODE_TTL_SECONDS", "300"))
CODE_LENGTH = int(os.getenv("CODE_LENGTH", "6"))
RESEND_COOLDOWN_SECONDS = int(os.getenv("RESEND_COOLDOWN_SECONDS", "30"))
SMTP_TIMEOUT_SECONDS = int(os.getenv("SMTP_TIMEOUT_SECONDS", "20"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
EMAIL_TRANSPORT = os.getenv("EMAIL_TRANSPORT", "resend").lower()

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "true").lower() == "true"

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_FROM = os.getenv("RESEND_FROM", "")
RESEND_API_URL = os.getenv("RESEND_API_URL", "https://api.resend.com/emails")

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
STATIC_DIR = Path(__file__).parent / "static"

# In-memory store only. No DB.
# Example: {"user@example.com": {"code": "123456", "expires": 1700000000.0, "last_sent": 1699999900.0}}
verification_store = {}


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_json(handler: BaseHTTPRequestHandler):
    content_length = int(handler.headers.get("Content-Length", "0"))
    if content_length == 0:
        return {}
    raw = handler.rfile.read(content_length)
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return None


def generate_code(length: int = CODE_LENGTH) -> str:
    return "".join(random.choice("0123456789") for _ in range(length))


def validate_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email or ""))


def build_email_bodies(code: str):
    ttl_minutes = max(1, CODE_TTL_SECONDS // 60)
    text_body = (
        f"Ваш код подтверждения: {code}\n\n"
        f"Код действует {ttl_minutes} минут."
    )
    html_body = f"""\
<!doctype html>
<html lang="ru">
  <body style="margin:0;padding:0;background:#f3f6fb;font-family:Arial,sans-serif;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding:24px 0;background:#f3f6fb;">
      <tr>
        <td align="center">
          <table role="presentation" width="560" cellpadding="0" cellspacing="0" style="width:560px;max-width:92%;background:#ffffff;border-radius:14px;overflow:hidden;border:1px solid #e5eaf2;">
            <tr>
              <td style="padding:28px 28px 20px;background:linear-gradient(135deg,#0b3954,#087ea4);color:#ffffff;">
                <h1 style="margin:0;font-size:22px;line-height:1.25;">Подтверждение email</h1>
                <p style="margin:10px 0 0;font-size:14px;opacity:.95;">Введите код ниже на странице регистрации</p>
              </td>
            </tr>
            <tr>
              <td style="padding:26px 28px 10px;color:#12202f;">
                <p style="margin:0 0 14px;font-size:15px;line-height:1.5;">Ваш код подтверждения:</p>
                <div style="display:inline-block;padding:12px 20px;font-size:30px;letter-spacing:6px;font-weight:700;background:#eff7ff;border:1px dashed #9ac4ea;border-radius:12px;color:#0d3d65;">
                  {code}
                </div>
                <p style="margin:16px 0 0;font-size:14px;color:#5a6878;line-height:1.5;">Код действует <strong>{ttl_minutes} минут</strong>.</p>
              </td>
            </tr>
            <tr>
              <td style="padding:18px 28px 28px;">
                <p style="margin:0;font-size:12px;color:#7d8896;line-height:1.6;">
                  Если вы не запрашивали этот код, просто проигнорируйте письмо.
                </p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""
    return text_body, html_body


def send_email_via_smtp(email: str, code: str):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and SMTP_FROM):
        raise RuntimeError(
            "SMTP is not configured. Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM"
        )

    text_body, html_body = build_email_bodies(code)
    msg = EmailMessage()
    msg["Subject"] = "Ваш код подтверждения"
    msg["From"] = SMTP_FROM
    msg["To"] = email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    if SMTP_USE_SSL:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            SMTP_HOST, SMTP_PORT, context=context, timeout=SMTP_TIMEOUT_SECONDS
        ) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)


def send_email_via_resend(email: str, code: str):
    if not (RESEND_API_KEY and RESEND_FROM):
        raise RuntimeError(
            "Resend is not configured. Set RESEND_API_KEY and RESEND_FROM"
        )

    text_body, html_body = build_email_bodies(code)
    payload = {
        "from": RESEND_FROM,
        "to": [email],
        "subject": "Ваш код подтверждения",
        "text": text_body,
        "html": html_body,
    }
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        RESEND_API_URL,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "YourApp/1.0 (+https://yourpasswordforemail.ru)",
        },
    )
    try:
        with urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status >= 400:
                raise RuntimeError(f"Resend API error {resp.status}: {body}")
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Resend API error {exc.code}: {detail}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"Resend connection error: {exc.reason}") from exc


def send_email_code(email: str, code: str):
    if EMAIL_TRANSPORT == "resend":
        send_email_via_resend(email, code)
        return

    if EMAIL_TRANSPORT == "smtp":
        send_email_via_smtp(email, code)
        return

    if EMAIL_TRANSPORT == "auto":
        errors = []
        try:
            send_email_via_resend(email, code)
            return
        except Exception as exc:
            errors.append(f"resend: {exc}")
        try:
            send_email_via_smtp(email, code)
            return
        except Exception as exc:
            errors.append(f"smtp: {exc}")
        raise RuntimeError("; ".join(errors))

    raise RuntimeError(
        "EMAIL_TRANSPORT must be one of: resend, smtp, auto"
    )


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Keep default server logs concise.
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            index_path = STATIC_DIR / "index.html"
            if not index_path.exists():
                self.send_error(404, "index.html not found")
                return
            content = index_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        self.send_error(404, "Not found")

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/api/send-code":
                return self.handle_send_code()
            if parsed.path == "/api/verify-code":
                return self.handle_verify_code()
            return json_response(self, 404, {"ok": False, "error": "Not found"})
        except Exception as exc:
            return json_response(
                self,
                500,
                {"ok": False, "error": f"Внутренняя ошибка сервера: {str(exc)}"},
            )

    def handle_send_code(self):
        data = read_json(self)
        if data is None:
            return json_response(self, 400, {"ok": False, "error": "Невалидный JSON"})

        email = (data.get("email") or "").strip().lower()
        if not validate_email(email):
            return json_response(self, 400, {"ok": False, "error": "Введите корректный email"})

        now = time.time()
        existing = verification_store.get(email)
        if existing and now - existing.get("last_sent", 0) < RESEND_COOLDOWN_SECONDS:
            wait = int(RESEND_COOLDOWN_SECONDS - (now - existing.get("last_sent", 0)))
            return json_response(
                self,
                429,
                {
                    "ok": False,
                    "error": f"Подождите {wait} сек. перед повторной отправкой",
                },
            )

        code = generate_code()
        try:
            send_email_code(email, code)
        except Exception as exc:
            return json_response(
                self,
                500,
                {"ok": False, "error": f"Ошибка отправки письма: {str(exc)}"},
            )

        verification_store[email] = {
            "code": code,
            "expires": now + CODE_TTL_SECONDS,
            "last_sent": now,
        }

        return json_response(
            self,
            200,
            {
                "ok": True,
                "message": "Код отправлен на вашу почту",
                "expires_in": CODE_TTL_SECONDS,
            },
        )

    def handle_verify_code(self):
        data = read_json(self)
        if data is None:
            return json_response(self, 400, {"ok": False, "error": "Невалидный JSON"})

        email = (data.get("email") or "").strip().lower()
        code = (data.get("code") or "").strip()

        if not validate_email(email):
            return json_response(self, 400, {"ok": False, "error": "Введите корректный email"})
        if not code:
            return json_response(self, 400, {"ok": False, "error": "Введите код"})

        entry = verification_store.get(email)
        if not entry:
            return json_response(self, 400, {"ok": False, "error": "Сначала запросите код"})

        now = time.time()
        if now > entry["expires"]:
            del verification_store[email]
            return json_response(self, 400, {"ok": False, "error": "Код истек"})

        if code != entry["code"]:
            return json_response(self, 400, {"ok": False, "error": "Неверный код"})

        del verification_store[email]
        return json_response(
            self,
            200,
            {"ok": True, "message": "Email успешно подтвержден"},
        )


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), AppHandler)
    print(f"Server started on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
