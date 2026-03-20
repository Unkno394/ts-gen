import type { GenerationResult, HistoryItem, UserProfile } from '../types';

type GenerateParams = {
  file: File;
  targetJson: string;
  userId?: string;
  selectedSheet?: string;
};

type BackendGenerateResponse = {
  generation_id: string | null;
  mode: 'guest' | 'authorized';
  parsed_file: {
    file_name: string;
    file_type: string;
    columns: string[];
    rows: Record<string, unknown>[];
    sheets: Array<{
      name: string;
      columns: string[];
      rows: Record<string, unknown>[];
    }>;
    warnings: string[];
  };
  mappings: Array<{
    source: string | null;
    target: string;
    confidence: 'high' | 'medium' | 'low' | 'none';
    reason: string;
  }>;
  generated_typescript: string;
  preview: Record<string, unknown>[];
  warnings: string[];
};

type BackendHistoryResponse = {
  items: Array<{
    id: string;
    user_id: string;
    file_name: string;
    file_type: string;
    target_json: Record<string, unknown>;
    mappings: Array<{
      source: string | null;
      target: string;
      confidence: 'high' | 'medium' | 'low' | 'none';
      reason: string;
    }>;
    generated_typescript: string;
    preview: Record<string, unknown>[];
    warnings: string[];
    created_at: string;
  }>;
};

type AuthParams = {
  email: string;
  password: string;
  name?: string;
  verificationCode?: string;
};

type BackendUserProfile = {
  id: string;
  name: string;
  email: string;
};

type BackendSendCodeResponse = {
  message: string;
  expires_in: number;
};

type BackendMessageResponse = {
  message: string;
};

type BackendVerifyResetCodeResponse = {
  message: string;
  reset_token: string;
};

const DEFAULT_BACKEND_URL = 'http://127.0.0.1:8000';
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const GENERATE_REQUEST_TIMEOUT_MS = 60_000;

function normalizeBackendError(detail: string, status?: number): string {
  const normalized = detail.trim();

  if (normalized === 'Invalid email or password.' || normalized === 'Неверный email или пароль.') {
    return 'Неверный email или пароль. Проверьте данные и попробуйте ещё раз.';
  }

  if (normalized === 'Email and password are required.' || normalized === 'Введите email и пароль.') {
    return 'Введите email и пароль.';
  }

  if (normalized === 'User with this email already exists.' || normalized === 'Пользователь с таким email уже зарегистрирован.') {
    return 'Этот email уже зарегистрирован. Попробуйте войти в аккаунт.';
  }

  if (normalized.includes('API key is invalid')) {
    return 'Сервис отправки писем настроен неправильно. Обратитесь к администратору или попробуйте позже.';
  }

  if (normalized.includes('Email transport is not configured') || normalized.includes('SMTP is not configured') || normalized.includes('Resend is not configured')) {
    return 'Сервис отправки писем сейчас не настроен. Попробуйте позже.';
  }

  if (normalized.startsWith('Resend API error')) {
    return 'Не удалось отправить письмо с кодом подтверждения. Попробуйте позже.';
  }

  if (normalized.startsWith('Resend connection error')) {
    return 'Нет соединения с сервисом отправки писем. Попробуйте позже.';
  }

  if (normalized.startsWith('Unexpected error:') || status === 500) {
    return 'Произошла внутренняя ошибка сервера. Попробуйте ещё раз.';
  }

  return normalized;
}

function resolveApiBaseUrl(): string {
  const envBaseUrl = (import.meta.env as ImportMetaEnv & { VITE_BACKEND_URL?: string }).VITE_BACKEND_URL?.trim();
  if (envBaseUrl) {
    return envBaseUrl.replace(/\/+$/, '');
  }

  const electronBaseUrl = window.electronAPI?.backendBaseUrl?.trim();
  if (electronBaseUrl) {
    return electronBaseUrl.replace(/\/+$/, '');
  }

  if (typeof window !== 'undefined' && (window.electronAPI || window.location.protocol === 'file:')) {
    return DEFAULT_BACKEND_URL;
  }

  return '';
}

function buildApiUrl(path: string): string {
  return `${resolveApiBaseUrl()}${path}`;
}

function parseConfidence(value: 'high' | 'medium' | 'low' | 'none'): 'high' | 'medium' | 'low' | 'none' {
  return value;
}

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    const detail = payload && typeof payload.detail === 'string' ? payload.detail : `Request failed: ${response.status}`;
    throw new Error(normalizeBackendError(detail, response.status));
  }
  return response.json() as Promise<T>;
}

async function fetchWithTimeout(input: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(input, {
      ...init,
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error(`Сервер не ответил за ${Math.round(timeoutMs / 1000)} сек. Попробуйте ещё раз.`);
    }
    throw new Error('Сервер сейчас недоступен. Проверьте, что backend запущен, и попробуйте ещё раз.');
  } finally {
    window.clearTimeout(timeoutId);
  }
}

async function postJson<T>(path: string, payload: Record<string, unknown>, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<T> {
  const response = await fetchWithTimeout(
    buildApiUrl(path),
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    },
    timeoutMs
  );
  return parseJson<T>(response);
}

export async function registerWithBackend({ email, password, name, verificationCode }: AuthParams): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/register', {
    email,
    password,
    name,
    verification_code: verificationCode,
  });
}

export async function loginWithBackend({ email, password }: AuthParams): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/login', {
    email,
    password,
  });
}

export async function requestRegistrationCode(email: string): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-code', {
    email,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function requestPasswordResetCode(email: string): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-reset-code', {
    email,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function requestEmailChangeCode(params: { userId: string; newEmail: string }): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-email-change-code', {
    user_id: params.userId,
    new_email: params.newEmail,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function resetPasswordWithBackend(params: { email: string; password: string; verificationCode: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/reset-password', {
    email: params.email,
    password: params.password,
    verification_code: params.verificationCode,
  });
  return response.message;
}

export async function verifyPasswordResetCode(email: string, verificationCode: string): Promise<{ message: string; resetToken: string }> {
  const response = await postJson<BackendVerifyResetCodeResponse>('/api/auth/verify-reset-code', {
    email,
    verification_code: verificationCode,
  });

  return {
    message: response.message,
    resetToken: response.reset_token,
  };
}

export async function completePasswordReset(params: { email: string; password: string; resetToken: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/reset-password', {
    email: params.email,
    password: params.password,
    reset_token: params.resetToken,
  });
  return response.message;
}

export async function changeEmailWithPassword(params: { userId: string; newEmail: string; currentPassword: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/change-email', {
    user_id: params.userId,
    new_email: params.newEmail,
    current_password: params.currentPassword,
  });
}

export async function changeEmailWithCode(params: { userId: string; newEmail: string; verificationCode: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/change-email', {
    user_id: params.userId,
    new_email: params.newEmail,
    verification_code: params.verificationCode,
  });
}

export async function updateProfileName(params: { userId: string; name: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/update-profile', {
    user_id: params.userId,
    name: params.name,
  });
}

export async function changePasswordWithBackend(params: { userId: string; currentPassword: string; newPassword: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/change-password', {
    user_id: params.userId,
    current_password: params.currentPassword,
    new_password: params.newPassword,
  });
  return response.message;
}

export async function generateFromBackend({ file, targetJson, userId, selectedSheet }: GenerateParams): Promise<GenerationResult> {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('target_json', targetJson);
  if (userId) {
    formData.append('user_id', userId);
  }
  if (selectedSheet) {
    formData.append('selected_sheet', selectedSheet);
  }

  const response = await fetchWithTimeout(
    buildApiUrl('/api/generate'),
    {
      method: 'POST',
      body: formData,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  const payload = await parseJson<BackendGenerateResponse>(response);
  return {
    generationId: payload.generation_id,
    parsedFile: {
      fileName: payload.parsed_file.file_name,
      extension: payload.parsed_file.file_type,
      columns: payload.parsed_file.columns,
      rows: payload.parsed_file.rows,
      sheets: payload.parsed_file.sheets ?? [],
      warnings: payload.parsed_file.warnings,
    },
    code: payload.generated_typescript,
    mappings: payload.mappings.map((item) => ({
      source: item.source ?? 'not found',
      target: item.target,
      confidence: parseConfidence(item.confidence),
      reason: item.reason,
    })),
    preview: payload.preview,
    warnings: payload.warnings,
  };
}

export async function fetchHistory(userId: string): Promise<HistoryItem[]> {
  const response = await fetchWithTimeout(
    buildApiUrl(`/api/history/${encodeURIComponent(userId)}`),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendHistoryResponse>(response);
  return payload.items.map((item) => ({
    id: item.id,
    createdAt: item.created_at,
    fileName: item.file_name,
    schema: JSON.stringify(item.target_json, null, 2),
    code: item.generated_typescript,
    mappings: item.mappings.map((mapping) => ({
      source: mapping.source ?? 'not found',
      target: mapping.target,
      confidence: parseConfidence(mapping.confidence),
      reason: mapping.reason,
    })),
    preview: item.preview,
    warnings: item.warnings,
  }));
}
