import { useEffect, useMemo, useState } from 'react';
import { AuthScreen } from './components/AuthScreen';
import { BrandLogo, SPLASH_LOGO_PATHS } from './components/BrandLogo';
import { Workspace } from './components/Workspace';
import { VibeBackground } from './components/VibeBackground';
import { fetchHistory } from './lib/api';
import type { HistoryItem, UserProfile } from './types';

const USER_KEY = 'tsgen.user';
const HISTORY_KEY = 'tsgen.history';
const SPLASH_MS = 3400;

function readJSON<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
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
  const [history, setHistory] = useState<HistoryItem[]>(() => readJSON<HistoryItem[]>(HISTORY_KEY, []));
  const [showSplash, setShowSplash] = useState(true);

  useEffect(() => {
    const timer = window.setTimeout(() => setShowSplash(false), SPLASH_MS);
    return () => window.clearTimeout(timer);
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadHistory() {
      if (!profile || profile.skipped) {
        setHistory([]);
        localStorage.removeItem(HISTORY_KEY);
        return;
      }

      try {
        const next = await fetchHistory(profile.id);
        if (!cancelled) {
          setHistory(next);
          localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
        }
      } catch (error) {
        if (!cancelled) {
          console.error(error);
        }
      }
    }

    void loadHistory();
    return () => {
      cancelled = true;
    };
  }, [profile]);

  const actions = useMemo(
    () => ({
      login(next: UserProfile) {
        localStorage.setItem(USER_KEY, JSON.stringify(next));
        setProfile(next);
      },
      logout() {
        localStorage.removeItem(USER_KEY);
        localStorage.removeItem(HISTORY_KEY);
        setHistory([]);
        setProfile(null);
      },
      async saveHistory() {
        if (!profile || profile.skipped) return;
        const next = await fetchHistory(profile.id);
        setHistory(next);
        localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
      },
      updateProfile(next: UserProfile) {
        localStorage.setItem(USER_KEY, JSON.stringify(next));
        setProfile(next);
      }
    }),
    [profile]
  );

  if (showSplash) {
    return <SplashScreen />;
  }

  if (!profile) {
    return <AuthScreen onComplete={actions.login} />;
  }

  return (
    <Workspace
      history={history}
      onLogout={actions.logout}
      onProfileUpdate={actions.updateProfile}
      onSaveHistory={actions.saveHistory}
      profile={profile}
    />
  );
}
