from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from typing import TypedDict

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from storage import UserNotFoundError, get_user_profile


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        normalized_key = key.strip()
        normalized_value = value.strip().strip('"').strip("'")
        if normalized_key and normalized_key not in os.environ:
            os.environ[normalized_key] = normalized_value


load_dotenv(Path(__file__).parent / '.env')

RUNTIME_DIR = Path(__file__).parent / '.runtime'
AUTH_SECRET_PATH = RUNTIME_DIR / 'auth_secret.txt'


class AuthenticatedUser(TypedDict):
    id: str
    name: str
    email: str


def _resolve_auth_secret() -> str:
    env_secret = os.getenv('TSGEN_AUTH_SECRET', '').strip()
    if env_secret:
        return env_secret

    try:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        if AUTH_SECRET_PATH.exists():
            persisted_secret = AUTH_SECRET_PATH.read_text(encoding='utf-8').strip()
            if persisted_secret:
                return persisted_secret
        generated_secret = secrets.token_hex(32)
        AUTH_SECRET_PATH.write_text(generated_secret, encoding='utf-8')
        return generated_secret
    except OSError:
        return 'tsgen-dev-secret-change-me'


AUTH_SECRET = _resolve_auth_secret()
ACCESS_TOKEN_TTL_SECONDS = max(60, int(os.getenv('TSGEN_ACCESS_TOKEN_TTL_SECONDS', '604800')))
AUTH_USER_CACHE_TTL_SECONDS = max(1, int(os.getenv('TSGEN_AUTH_USER_CACHE_TTL_SECONDS', '30')))

_bearer_scheme = HTTPBearer(auto_error=False)
_user_cache: dict[str, tuple[float, AuthenticatedUser]] = {}


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def _base64url_decode(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(f'{value}{padding}')


def _auth_error(detail: str = 'Authentication required.') -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={'WWW-Authenticate': 'Bearer'},
    )


def create_access_token(user_id: str) -> str:
    normalized_user_id = user_id.strip()
    if not normalized_user_id:
        raise ValueError('user_id is required for token creation')

    now = int(time.time())
    header = {'alg': 'HS256', 'typ': 'JWT'}
    payload = {
        'sub': normalized_user_id,
        'iat': now,
        'exp': now + ACCESS_TOKEN_TTL_SECONDS,
    }
    encoded_header = _base64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))
    encoded_payload = _base64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
    signing_input = f'{encoded_header}.{encoded_payload}'.encode('ascii')
    signature = hmac.new(AUTH_SECRET.encode('utf-8'), signing_input, hashlib.sha256).digest()
    return f'{encoded_header}.{encoded_payload}.{_base64url_encode(signature)}'


def invalidate_user_cache(user_id: str | None) -> None:
    if not user_id:
        return
    _user_cache.pop(user_id.strip(), None)


def _decode_access_token(token: str) -> str:
    segments = token.split('.')
    if len(segments) != 3:
        raise _auth_error('Invalid access token.')

    encoded_header, encoded_payload, encoded_signature = segments
    signing_input = f'{encoded_header}.{encoded_payload}'.encode('ascii')
    expected_signature = hmac.new(AUTH_SECRET.encode('utf-8'), signing_input, hashlib.sha256).digest()

    try:
        provided_signature = _base64url_decode(encoded_signature)
    except Exception as exc:  # noqa: BLE001
        raise _auth_error('Invalid access token.') from exc

    if not hmac.compare_digest(expected_signature, provided_signature):
        raise _auth_error('Invalid access token.')

    try:
        payload = json.loads(_base64url_decode(encoded_payload).decode('utf-8'))
    except Exception as exc:  # noqa: BLE001
        raise _auth_error('Invalid access token.') from exc

    subject = str(payload.get('sub') or '').strip()
    expires_at = payload.get('exp')

    if not subject:
        raise _auth_error('Invalid access token.')
    if not isinstance(expires_at, (int, float)) or int(expires_at) <= int(time.time()):
        raise _auth_error('Session expired. Please sign in again.')

    return subject


def _load_authenticated_user(user_id: str) -> AuthenticatedUser:
    cached = _user_cache.get(user_id)
    now = time.time()
    if cached and cached[0] > now:
        return cached[1]

    try:
        profile = get_user_profile(user_id)
    except UserNotFoundError as exc:
        raise _auth_error('User not found for this session.') from exc

    authenticated_user: AuthenticatedUser = {
        'id': profile['id'],
        'name': profile['name'],
        'email': profile['email'],
    }
    _user_cache[user_id] = (now + AUTH_USER_CACHE_TTL_SECONDS, authenticated_user)
    return authenticated_user


def _resolve_authenticated_user(credentials: HTTPAuthorizationCredentials | None) -> AuthenticatedUser | None:
    if credentials is None:
        return None
    if credentials.scheme.lower() != 'bearer':
        raise _auth_error('Unsupported authentication scheme.')

    user_id = _decode_access_token(credentials.credentials)
    return _load_authenticated_user(user_id)


def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme)) -> AuthenticatedUser:
    user = _resolve_authenticated_user(credentials)
    if user is None:
        raise _auth_error()
    return user


def get_optional_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> AuthenticatedUser | None:
    return _resolve_authenticated_user(credentials)
