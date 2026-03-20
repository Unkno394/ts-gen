import { ArrowRight, KeyRound, LogIn, Mail, ShieldCheck, Sparkles, UserPlus, UserRound } from 'lucide-react';
import { useMemo, useState } from 'react';
import type { FormEvent } from 'react';
import { loginWithBackend, registerWithBackend, requestRegistrationCode } from '../lib/api';
import type { AuthMode, UserProfile } from '../types';
import { BrandLogo } from './BrandLogo';
import { VibeBackground } from './VibeBackground';

type Props = {
  onComplete: (profile: UserProfile) => void;
};

type FieldProps = {
  icon: typeof UserRound;
  label: string;
  placeholder: string;
  type?: string;
  value: string;
  autoComplete?: string;
  onChange: (value: string) => void;
};

function Field({ icon: Icon, label, placeholder, type = 'text', value, autoComplete, onChange }: FieldProps) {
  return (
    <label className="auth-field">
      <span className="auth-field-label">{label}</span>
      <div className="auth-input-wrap">
        <Icon size={18} />
        <input
          autoComplete={autoComplete}
          onChange={(event) => onChange(event.target.value)}
          placeholder={placeholder}
          type={type}
          value={value}
        />
      </div>
    </label>
  );
}

export function AuthScreen({ onComplete }: Props) {
  const [mode, setMode] = useState<AuthMode>('register');
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [verificationCode, setVerificationCode] = useState('');
  const [busy, setBusy] = useState(false);
  const [sendingCode, setSendingCode] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [verificationEmail, setVerificationEmail] = useState('');
  const verificationRequested = mode === 'register' && Boolean(verificationEmail);

  const title = mode === 'register' ? 'Регистрация' : 'Войти в аккаунт';
  const helperText = useMemo(() => {
    if (mode === 'register') {
      return 'Регистрация подтверждается кодом из письма. После этого история генераций и рабочие данные сохраняются в общей базе проекта.';
    }
    return 'Войдите в существующий аккаунт, чтобы продолжить работу с сохраненной историей.';
  }, [mode]);

  const resetRegisterVerification = () => {
    setVerificationCode('');
    setVerificationEmail('');
    setNotice('');
  };

  const handleModeChange = (nextMode: AuthMode) => {
    setMode(nextMode);
    setError('');
    setNotice('');

    if (nextMode === 'login') {
      resetRegisterVerification();
    }
  };

  const handleEmailChange = (value: string) => {
    setEmail(value);

    if (mode !== 'register') {
      return;
    }

    const normalized = value.trim().toLowerCase();
    if (verificationEmail && normalized !== verificationEmail) {
      resetRegisterVerification();
    }
  };

  const sendCode = async () => {
    const normalizedEmail = email.trim().toLowerCase();
    if (!normalizedEmail) {
      setError('Введите email, на который нужно отправить код.');
      return;
    }

    setSendingCode(true);
    setError('');
    setNotice('');

    try {
      const response = await requestRegistrationCode(normalizedEmail);
      const ttlMinutes = Math.max(1, Math.ceil(response.expiresIn / 60));
      setVerificationEmail(normalizedEmail);
      setNotice(`${response.message} Код действует ${ttlMinutes} мин.`);
    } catch (sendCodeError) {
      setError(sendCodeError instanceof Error ? sendCodeError.message : 'Не удалось отправить письмо.');
    } finally {
      setSendingCode(false);
    }
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError('');
    setBusy(true);

    try {
      const normalizedEmail = email.trim().toLowerCase();
      const normalizedPassword = password.trim();

      if (mode === 'register') {
        if (!verificationRequested) {
          if (!name.trim()) {
            throw new Error('Введите имя.');
          }
          if (!normalizedEmail) {
            throw new Error('Введите email.');
          }
          if (!normalizedPassword) {
            throw new Error('Введите пароль.');
          }

          const response = await requestRegistrationCode(normalizedEmail);
          const ttlMinutes = Math.max(1, Math.ceil(response.expiresIn / 60));
          setVerificationEmail(normalizedEmail);
          setNotice(`${response.message} Код действует ${ttlMinutes} мин.`);
          return;
        }

        if (!verificationCode.trim()) {
          throw new Error('Введите код из письма.');
        }
        if (verificationEmail !== normalizedEmail) {
          throw new Error('Сначала отправьте код подтверждения на этот email.');
        }
      }

      const profile =
        mode === 'register'
          ? await registerWithBackend({
              name: name.trim(),
              email: normalizedEmail,
              password: normalizedPassword,
              verificationCode: verificationCode.trim(),
            })
          : await loginWithBackend({
              email: normalizedEmail,
              password: normalizedPassword,
            });

      resetRegisterVerification();
      onComplete({
        ...profile,
        skipped: false,
      });
    } catch (submissionError) {
      setError(submissionError instanceof Error ? submissionError.message : 'Не удалось выполнить вход.');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="auth-shell auth-shell-v2 auth-shell-redesign">
      <VibeBackground className="auth-scene auth-scene-full" baseScale={0.86} energy={0.22} />
      <div className="auth-noise" />
      <div className="auth-orb auth-orb-top" />
      <div className="auth-orb auth-orb-bottom" />

      <section className="auth-layout auth-layout-redesign">
        <aside className="auth-hero auth-hero-redesign">
          <div className="brand-row brand-row-v2 brand-row-redesign">
            <div className="brand-badge brand-badge-v2 brand-badge-mark">
              <BrandLogo className="brand-mark" />
            </div>
            <div>
              <div className="eyebrow">Electron workspace</div>
              <h1>TSGen</h1>
            </div>
          </div>

          <div className="auth-hero-copy">
            <p className="auth-kicker">Обновленная сессия</p>
            <h2>Один проект, одна база, нормальная история генераций.</h2>
            <p className="subtle-text auth-hero-text">
              Приложение больше не держит авторизацию только локально. Пользовательские аккаунты и история генераций теперь
              живут в SQLite-слое проекта и могут использовать общую доменную модель.
            </p>
          </div>

          <div className="auth-feature-list glass-card auth-feature-list-redesign">
            <div className="auth-feature-item">
              <div className="auth-feature-icon">
                <ShieldCheck size={16} />
              </div>
              <div>
                <strong>Реальные пользователи</strong>
                <span>Регистрация и вход теперь идут через backend и сохраняются в общей базе данных.</span>
              </div>
            </div>
            <div className="auth-feature-item">
              <div className="auth-feature-icon">
                <Sparkles size={16} />
              </div>
              <div>
                <strong>Единая история</strong>
                <span>Генерации, версии и метаданные сохраняются рядом с остальными сущностями проекта.</span>
              </div>
            </div>
          </div>
        </aside>

        <div className="entry-grid">
          <section className="auth-card auth-card-v2 glass-card auth-card-redesign">
            <div className="auth-card-top">
              <div>
                <div className="eyebrow">Account access</div>
                <h3>{title}</h3>
              </div>
            </div>

            <p className="subtle-text auth-copy auth-copy-v2">{helperText}</p>

            <div className="mode-switch mode-switch-v2">
              <button
                className={mode === 'register' ? 'active' : ''}
                onClick={() => {
                  handleModeChange('register');
                }}
                type="button"
              >
                Регистрация
              </button>
              <button
                className={mode === 'login' ? 'active' : ''}
                onClick={() => {
                  handleModeChange('login');
                }}
                type="button"
              >
                Вход
              </button>
            </div>

            <form className="auth-form auth-form-v2" onSubmit={submit}>
              <div className={mode === 'register' ? 'auth-field-animated expanded' : 'auth-field-animated collapsed'}>
                <Field
                  autoComplete="name"
                  icon={UserRound}
                  label="Имя"
                  onChange={setName}
                  placeholder="Например, Алина Воронцова"
                  value={name}
                />
              </div>

              <Field
                autoComplete="email"
                icon={Mail}
                label="Email"
                onChange={handleEmailChange}
                placeholder="name@company.com"
                type="email"
                value={email}
              />

              <Field
                autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
                icon={KeyRound}
                label="Пароль"
                onChange={setPassword}
                placeholder="Минимум 8 символов"
                type="password"
                value={password}
              />

              <div className={verificationRequested ? 'auth-field-animated expanded' : 'auth-field-animated collapsed'}>
                <label className="auth-field auth-code-field">
                  <span className="auth-field-label">Код подтверждения</span>
                  <div className="auth-code-inline">
                    <div className="auth-input-wrap auth-code-input">
                      <ShieldCheck size={18} />
                      <input
                        autoComplete="one-time-code"
                        onChange={(event) => setVerificationCode(event.target.value)}
                        placeholder="6 цифр из письма"
                        type="text"
                        value={verificationCode}
                      />
                    </div>
                    <button
                      className="secondary-btn auth-code-btn auth-code-btn-inline"
                      disabled={sendingCode}
                      onClick={sendCode}
                      type="button"
                    >
                      <Mail size={16} />
                      <span>{sendingCode ? 'Отправляем...' : 'Отправить код'}</span>
                    </button>
                  </div>
                  <span className="auth-code-caption">
                    Поле и кнопка появляются только после первого нажатия на регистрацию.
                  </span>
                </label>
              </div>

              {notice && <div className="auth-status auth-status-success">{notice}</div>}
              {error && <div className="warning-item auth-status auth-status-error">{error}</div>}

              <button className="primary-btn primary-btn-v2" disabled={busy} type="submit">
                {mode === 'register' ? <UserPlus size={16} /> : <LogIn size={16} />}
                <span>
                  {busy
                    ? 'Подождите...'
                    : mode === 'register'
                      ? verificationRequested
                        ? 'Завершить регистрацию'
                        : 'Зарегистрироваться'
                      : 'Войти'}
                </span>
              </button>
            </form>

            <button
              className="register-cta ghost-btn ghost-btn-v2"
              onClick={() => onComplete({ id: crypto.randomUUID(), name: 'Guest', email: 'guest@local', skipped: true })}
              type="button"
            >
              <ArrowRight size={16} />
              Войти без регистрации
            </button>
          </section>
        </div>
      </section>
    </div>
  );
}
