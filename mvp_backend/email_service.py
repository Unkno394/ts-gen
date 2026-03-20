from __future__ import annotations

import json
import logging
import os
import random
import re
import smtplib
import ssl
import time
from email.message import EmailMessage
from html import escape
from pathlib import Path
from urllib import error as urlerror
from urllib.request import Request, urlopen

EMAIL_RE = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        normalized_key = key.strip()
        normalized_value = value.strip().strip("'").strip('"')
        if normalized_key and normalized_key not in os.environ:
            os.environ[normalized_key] = normalized_value


load_dotenv(Path(__file__).parent / '.env')

CODE_TTL_SECONDS = int(os.getenv('CODE_TTL_SECONDS', '300'))
CODE_LENGTH = int(os.getenv('CODE_LENGTH', '6'))
RESEND_COOLDOWN_SECONDS = int(os.getenv('RESEND_COOLDOWN_SECONDS', '30'))
SMTP_TIMEOUT_SECONDS = int(os.getenv('SMTP_TIMEOUT_SECONDS', '20'))
HTTP_TIMEOUT_SECONDS = int(os.getenv('HTTP_TIMEOUT_SECONDS', '20'))
EMAIL_TRANSPORT = os.getenv('EMAIL_TRANSPORT', 'auto').strip().lower()

SMTP_HOST = os.getenv('SMTP_HOST', '').strip()
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
SMTP_USER = os.getenv('SMTP_USER', '').strip()
SMTP_PASS = os.getenv('SMTP_PASS', '').strip()
SMTP_FROM = os.getenv('SMTP_FROM', SMTP_USER).strip()
SMTP_USE_SSL = os.getenv('SMTP_USE_SSL', 'true').strip().lower() == 'true'

RESEND_API_KEY = os.getenv('RESEND_API_KEY', '').strip()
RESEND_FROM = os.getenv('RESEND_FROM', '').strip()
RESEND_API_URL = os.getenv('RESEND_API_URL', 'https://api.resend.com/emails').strip()

verification_store: dict[str, dict[str, float | str]] = {}
logger = logging.getLogger(__name__)


class EmailServiceError(RuntimeError):
    status_code = 400


class EmailVerificationError(EmailServiceError):
    status_code = 400


class EmailRateLimitError(EmailServiceError):
    status_code = 429


class EmailDeliveryError(EmailServiceError):
    status_code = 502


def normalize_email(email: str) -> str:
    return email.strip().lower()


def validate_email(email: str) -> bool:
    return bool(EMAIL_RE.match(normalize_email(email)))


def generate_code(length: int = CODE_LENGTH) -> str:
    return ''.join(random.choice('0123456789') for _ in range(length))


def build_email_bodies(code: str) -> tuple[str, str]:
    ttl_minutes = max(1, CODE_TTL_SECONDS // 60)
    safe_code = escape(code)

    text_body = (
        'TSGen\n\n'
        f'Ваш код подтверждения: {code}\n'
        f'Введите его в окне регистрации. Код действует {ttl_minutes} минут.\n\n'
        'Если вы не запрашивали код, просто проигнорируйте это письмо.'
    )

    html_body = f"""\
<!doctype html>
<html lang="ru">
  <body style="margin:0;padding:0;background-color:#050711;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Arial,sans-serif;color:#f4f6fb;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;background:#050711;background-image:linear-gradient(180deg,#050711 0%,#0a1020 48%,#090d18 100%);">
      <tr>
        <td align="center" style="padding:32px 16px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:620px;border-spacing:0;">
            <tr>
              <td style="padding:0 0 16px 0;">
                <div style="display:inline-block;padding:8px 12px;border-radius:999px;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.08);font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:rgba(255,255,255,0.68);">
                  TSGen Access
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:0;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-spacing:0;border:1px solid rgba(255,255,255,0.08);border-radius:30px;overflow:hidden;background:rgba(11,15,27,0.96);box-shadow:0 32px 96px rgba(0,0,0,0.42);">
                  <tr>
                    <td style="padding:32px;border-bottom:1px solid rgba(255,255,255,0.08);background:linear-gradient(135deg,rgba(107,92,255,0.3),rgba(42,179,255,0.18));">
                      <div style="margin:0 0 8px 0;font-size:12px;letter-spacing:0.18em;text-transform:uppercase;color:rgba(255,255,255,0.62);">
                        Electron Workspace
                      </div>
                      <h1 style="margin:0;font-size:34px;line-height:1;color:#ffffff;font-weight:800;">
                        Код подтверждения для TSGen
                      </h1>
                      <p style="margin:16px 0 0 0;font-size:16px;line-height:1.7;color:rgba(244,246,251,0.78);">
                        Завершите регистрацию в приложении и введите код из этого письма.
                      </p>
                    </td>
                  </tr>
                  <tr>
                    <td style="padding:30px 32px 18px 32px;">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-spacing:0;">
                        <tr>
                          <td style="padding:0 0 16px 0;font-size:14px;line-height:1.7;color:rgba(255,255,255,0.72);">
                            Один проект, одна база, нормальная история генераций. Для входа используйте этот код:
                          </td>
                        </tr>
                        <tr>
                          <td align="center" style="padding:8px 0 10px 0;">
                            <div style="display:inline-block;min-width:220px;padding:18px 28px;border-radius:22px;background:linear-gradient(135deg,rgba(107,92,255,0.2),rgba(42,179,255,0.14));border:1px solid rgba(143,170,255,0.24);font-size:34px;font-weight:800;letter-spacing:0.42em;text-indent:0.42em;color:#ffffff;">
                              {safe_code}
                            </div>
                          </td>
                        </tr>
                        <tr>
                          <td style="padding:14px 0 0 0;font-size:14px;line-height:1.7;color:rgba(255,255,255,0.62);">
                            Код действует <strong style="color:#ffffff;">{ttl_minutes} минут</strong>. Если вы не запрашивали письмо, ничего делать не нужно.
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                  <tr>
                    <td style="padding:0 32px 32px 32px;">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-spacing:0;">
                        <tr>
                          <td style="padding:16px 18px;border-radius:18px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.06);font-size:13px;line-height:1.7;color:rgba(255,255,255,0.56);">
                            Письмо отправлено автоматически из backend TSGen. Не отвечайте на него.
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
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


def _configured_transports() -> list[str]:
    transports: list[str] = []

    if RESEND_API_KEY and RESEND_FROM:
        transports.append('resend')
    if SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and SMTP_FROM:
        transports.append('smtp')

    return transports


def _resolve_transport_order() -> list[str]:
    if EMAIL_TRANSPORT in {'smtp', 'resend'}:
        return [EMAIL_TRANSPORT]

    if EMAIL_TRANSPORT == 'auto':
        configured = _configured_transports()
        if configured:
            return configured
        raise EmailDeliveryError(
            'Email transport is not configured. Set SMTP_* or RESEND_* variables in mvp_backend/.env.'
        )

    raise EmailDeliveryError('EMAIL_TRANSPORT must be one of: resend, smtp, auto.')


def send_email_via_smtp(email: str, code: str) -> None:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and SMTP_FROM):
        raise EmailDeliveryError(
            'SMTP is not configured. Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM.'
        )

    logger.info('email transport smtp: sending code to=%s from=%s host=%s port=%s', email, SMTP_FROM, SMTP_HOST, SMTP_PORT)
    text_body, html_body = build_email_bodies(code)
    message = EmailMessage()
    message['Subject'] = 'Код подтверждения TSGen'
    message['From'] = SMTP_FROM
    message['To'] = email
    message.set_content(text_body)
    message.add_alternative(html_body, subtype='html')

    if SMTP_USE_SSL:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(message)
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
        server.starttls(context=ssl.create_default_context())
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(message)


def send_email_via_resend(email: str, code: str) -> None:
    if not (RESEND_API_KEY and RESEND_FROM):
        raise EmailDeliveryError('Resend is not configured. Set RESEND_API_KEY and RESEND_FROM.')

    logger.info('email transport resend: sending code to=%s from=%s url=%s', email, RESEND_FROM, RESEND_API_URL)
    text_body, html_body = build_email_bodies(code)
    payload = json.dumps(
        {
            'from': RESEND_FROM,
            'to': [email],
            'subject': 'Код подтверждения TSGen',
            'text': text_body,
            'html': html_body,
        }
    )

    request = Request(
        RESEND_API_URL,
        data=payload.encode('utf-8'),
        method='POST',
        headers={
            'Authorization': f'Bearer {RESEND_API_KEY}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'TSGen Desktop',
        },
    )

    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body = response.read().decode('utf-8', errors='replace')
            logger.info('email transport resend: response status=%s body=%s', response.status, body[:500])
            if response.status >= 400:
                raise EmailDeliveryError(f'Resend API error {response.status}: {body}')
    except urlerror.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        logger.error('email transport resend failed: status=%s body=%s', exc.code, detail[:1000])
        raise EmailDeliveryError(f'Resend API error {exc.code}: {detail}') from exc
    except urlerror.URLError as exc:
        logger.error('email transport resend failed: connection error=%s', exc.reason)
        raise EmailDeliveryError(f'Resend connection error: {exc.reason}') from exc


def send_email_code(email: str, code: str) -> None:
    errors: list[str] = []
    transport_order = _resolve_transport_order()
    logger.info('email delivery start: to=%s transport_order=%s', email, ','.join(transport_order))

    for transport in transport_order:
        try:
            if transport == 'resend':
                send_email_via_resend(email, code)
            else:
                send_email_via_smtp(email, code)
            logger.info('email delivery success: to=%s transport=%s', email, transport)
            return
        except EmailDeliveryError as exc:
            logger.warning('email delivery failed: to=%s transport=%s error=%s', email, transport, exc)
            errors.append(f'{transport}: {exc}')

    logger.error('email delivery exhausted: to=%s errors=%s', email, '; '.join(errors))
    raise EmailDeliveryError('; '.join(errors) if errors else 'No configured email transports are available.')


def request_registration_code(email: str) -> dict[str, int | str]:
    normalized_email = normalize_email(email)
    if not validate_email(normalized_email):
        raise EmailVerificationError('Введите корректный email.')

    now = time.time()
    entry = verification_store.get(normalized_email)
    if entry and now - float(entry.get('last_sent', 0)) < RESEND_COOLDOWN_SECONDS:
        wait_seconds = int(RESEND_COOLDOWN_SECONDS - (now - float(entry.get('last_sent', 0))))
        raise EmailRateLimitError(f'Подождите {wait_seconds} сек. перед повторной отправкой.')

    code = generate_code()
    logger.info('registration code generated: to=%s length=%s', normalized_email, len(code))
    send_email_code(normalized_email, code)

    verification_store[normalized_email] = {
        'code': code,
        'expires': now + CODE_TTL_SECONDS,
        'last_sent': now,
    }

    return {
        'message': 'Код отправлен на почту.',
        'expires_in': CODE_TTL_SECONDS,
    }


def consume_registration_code(email: str, code: str) -> None:
    normalized_email = normalize_email(email)
    normalized_code = code.strip()

    if not validate_email(normalized_email):
        raise EmailVerificationError('Введите корректный email.')
    if not normalized_code:
        raise EmailVerificationError('Введите код из письма.')

    entry = verification_store.get(normalized_email)
    if entry is None:
        raise EmailVerificationError('Сначала запросите код подтверждения.')

    now = time.time()
    if now > float(entry['expires']):
        verification_store.pop(normalized_email, None)
        raise EmailVerificationError('Срок действия кода истек. Запросите новый.')

    if normalized_code != str(entry['code']):
        raise EmailVerificationError('Неверный код подтверждения.')

    verification_store.pop(normalized_email, None)
