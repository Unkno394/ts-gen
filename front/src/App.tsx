import { Suspense, lazy, useEffect, useMemo, useState } from 'react';
import { BrandLogo, SPLASH_LOGO_PATHS } from './components/BrandLogo';
import { VibeBackground } from './components/VibeBackground';
import { clearAuthToken, fetchCurrentProfile, fetchHistory } from './lib/api';
import type { HistoryItem, UserProfile } from './types';

const USER_KEY = 'tsgen.user';
const HISTORY_KEY = 'tsgen.history';
const SPLASH_MS = 3400;
const AuthScreen = lazy(() => import('./components/AuthScreen').then((module) => ({ default: module.AuthScreen })));
const Workspace = lazy(() => import('./components/Workspace').then((module) => ({ default: module.Workspace })));

function readJSON<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function safeSetLocalStorage(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch (error) {
    console.error(error);
    if (key === HISTORY_KEY) {
      try {
        localStorage.removeItem(HISTORY_KEY);
      } catch {
        // Ignore storage cleanup failures.
      }
    }
  }
}

function isUnauthorizedError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'status' in error &&
    Number((error as { status?: unknown }).status) === 401
  );
}

function SplashScreen() {
  const [activeFrame, setActiveFrame] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setActiveFrame((current) => (current + 1) % SPLASH_LOGO_PATHS.length);
    }, 560);

    return () => window.clearInterval(timer);
  }, []);

  return (
    <div className="splash-screen">
      <VibeBackground className="splash-scene" baseScale={0.92} energy={0.24} />
      <div className="workspace-overlay" />
      <div className="splash-center glass-card">
        <div className="splash-logo-orbit" aria-hidden="true">
          <div className="splash-icon-wrap">
            {SPLASH_LOGO_PATHS.map((path, index) => (
              <BrandLogo
                key={`${index}-${path.slice(0, 18)}`}
                className={index === activeFrame ? 'splash-icon splash-icon-active' : 'splash-icon splash-icon-ghost'}
                path={path}
              />
            ))}
          </div>
        </div>
        <div className="splash-copy">
          <div className="eyebrow">Electron app</div>
          <h1>TSGen</h1>
          <p className="subtle-text">Запускаем рабочее пространство и подготавливаем визуальную сцену.</p>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const [profile, setProfile] = useState<UserProfile | null>(() => readJSON<UserProfile | null>(USER_KEY, null));
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [showSplash, setShowSplash] = useState(true);

  useEffect(() => {
    const timer = window.setTimeout(() => setShowSplash(false), SPLASH_MS);
    return () => window.clearTimeout(timer);
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadSessionData() {
      if (!profile || profile.skipped) {
        setHistory([]);
        localStorage.removeItem(HISTORY_KEY);
        return;
      }

      try {
        const nextProfile = await fetchCurrentProfile();
        if (cancelled) {
          return;
        }

        safeSetLocalStorage(USER_KEY, JSON.stringify(nextProfile));
        setProfile(nextProfile);
      } catch (error) {
        if (!cancelled) {
          console.error(error);
          if (isUnauthorizedError(error)) {
            clearAuthToken();
            localStorage.removeItem(USER_KEY);
            localStorage.removeItem(HISTORY_KEY);
            setHistory([]);
            setProfile(null);
          }
        }
        return;
      }

      try {
        const next = await fetchHistory(profile.id);
        if (!cancelled) {
          setHistory(next);
          try {
            localStorage.removeItem(HISTORY_KEY);
          } catch {
            // Ignore storage cleanup failures.
          }
        }
      } catch (error) {
        if (!cancelled) {
          console.error(error);
          if (isUnauthorizedError(error)) {
            clearAuthToken();
            localStorage.removeItem(USER_KEY);
            localStorage.removeItem(HISTORY_KEY);
            setHistory([]);
            setProfile(null);
          }
        }
      }
    }

    void loadSessionData();
    return () => {
      cancelled = true;
    };
  }, [profile?.id, profile?.skipped]);

  const actions = useMemo(
    () => ({
      login(next: UserProfile) {
        safeSetLocalStorage(USER_KEY, JSON.stringify(next));
        setProfile(next);
      },
      logout() {
        clearAuthToken();
        localStorage.removeItem(USER_KEY);
        localStorage.removeItem(HISTORY_KEY);
        setHistory([]);
        setProfile(null);
      },
      async saveHistory() {
        if (!profile || profile.skipped) return;
        const next = await fetchHistory(profile.id);
        setHistory(next);
        try {
          localStorage.removeItem(HISTORY_KEY);
        } catch {
          // Ignore storage cleanup failures.
        }
      },
      updateProfile(next: UserProfile) {
        safeSetLocalStorage(USER_KEY, JSON.stringify(next));
        setProfile(next);
      }
    }),
    [profile]
  );

  if (showSplash) {
    return <SplashScreen />;
  }

  if (!profile) {
    return (
      <Suspense fallback={<SplashScreen />}>
        <AuthScreen onComplete={actions.login} />
      </Suspense>
    );
  }

  return (
    <Suspense fallback={<SplashScreen />}>
      <Workspace
        history={history}
        onLogout={actions.logout}
        onProfileUpdate={actions.updateProfile}
        onSaveHistory={actions.saveHistory}
        profile={profile}
      />
    </Suspense>
  );
}
