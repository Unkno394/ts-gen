import {
  Check,
  Copy,
  Download,
  Eye,
  EyeOff,
  FileSpreadsheet,
  History,
  KeyRound,
  Mail,
  LockKeyhole,
  LogOut,
  ShieldCheck,
  Sparkles,
  SquarePen,
  TriangleAlert,
  Upload,
  UserRound,
  WandSparkles,
  X,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import * as XLSX from 'xlsx';
import {
  changeEmailWithCode,
  changeEmailWithPassword,
  changePasswordWithBackend,
  generateFromBackend,
  requestEmailChangeCode,
  updateProfileName,
} from '../lib/api';
import type { GenerationResult, HistoryItem, ParsedFileInfo, ParsedSheetInfo, UserProfile } from '../types';
import { VibeBackground } from './VibeBackground';

type Props = {
  profile: UserProfile;
  history: HistoryItem[];
  onLogout: () => void;
  onProfileUpdate: (profile: UserProfile) => void;
  onSaveHistory: () => Promise<void>;
};

const defaultSchema = `{
  "customerName": "",
  "amount": 0,
  "createdAt": ""
}`;

const defaultCode = `// Generated TypeScript will appear here
export function transform(row: any) {
  return {};
}`;

function buildPreviewSheet(name: string, columns: string[], rows: Record<string, unknown>[]): ParsedSheetInfo {
  return {
    name,
    columns,
    rows,
  };
}

function parseWorkbookSheets(workbook: XLSX.WorkBook): {
  columns: string[];
  rows: Record<string, unknown>[];
  sheets: ParsedSheetInfo[];
  warnings: string[];
} {
  const sheets = workbook.SheetNames.map((sheetName) => {
    const sheet = workbook.Sheets[sheetName];
    const json = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: '' });
    const columns = Object.keys(json[0] ?? {});
    const rows = json.slice(0, 8).map((row) => row as Record<string, string | number | boolean | null>);
    return buildPreviewSheet(sheetName, columns, rows);
  }).filter((sheet) => sheet.columns.length > 0 || sheet.rows.length > 0);

  const firstSheet = sheets[0] ?? buildPreviewSheet(workbook.SheetNames[0] ?? 'Sheet 1', [], []);
  const warnings: string[] = [];

  if (workbook.SheetNames.length > 1) {
    warnings.push(`Preview is split by sheets. Found ${workbook.SheetNames.length} sheet(s).`);
  }

  if (sheets.length === 0) {
    warnings.push('No previewable rows were found in the workbook.');
  }

  return {
    columns: firstSheet.columns,
    rows: firstSheet.rows,
    sheets,
    warnings,
  };
}

type PasswordInputProps = {
  icon: typeof LockKeyhole;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
};

function PasswordInput({ icon: Icon, placeholder, value, onChange }: PasswordInputProps) {
  const [visible, setVisible] = useState(false);

  return (
    <div className="auth-input-wrap">
      <Icon size={18} />
      <input
        placeholder={placeholder}
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
      <button
        className="password-toggle"
        onClick={() => setVisible((current) => !current)}
        tabIndex={-1}
        title={visible ? 'Скрыть пароль' : 'Показать пароль'}
        type="button"
      >
        {visible ? <EyeOff size={18} /> : <Eye size={18} />}
      </button>
    </div>
  );
}

async function parseFile(file: File): Promise<ParsedFileInfo> {
  const extension = file.name.split('.').pop()?.toLowerCase() ?? 'unknown';

  if (extension === 'csv') {
    const text = await file.text();
    const lines = text.split(/\r?\n/).filter((line) => line.trim() !== '');
    const [headerLine = '', ...dataLines] = lines;
    const columns = headerLine ? headerLine.split(',').map((item) => item.trim()) : [];
    const rows = dataLines.slice(0, 8).map((line) => {
      const cells = line.split(',');
      return Object.fromEntries(columns.map((column, index) => [column, cells[index] ?? '']));
    });

    return {
      fileName: file.name,
      extension,
      columns,
      rows,
      sheets: [buildPreviewSheet(file.name, columns, rows)],
      warnings: rows.length === 0 ? ['В файле нет строк данных.'] : [],
    };
  }

  if (extension === 'xlsx' || extension === 'xls') {
    const buffer = await file.arrayBuffer();
    const workbook = XLSX.read(buffer, { type: 'array' });
    const workbookPreview = parseWorkbookSheets(workbook);

    return {
      fileName: file.name,
      extension,
      columns: workbookPreview.columns,
      rows: workbookPreview.rows,
      sheets: workbookPreview.sheets,
      warnings: workbookPreview.warnings,
    };
  }

  if (extension === 'pdf' || extension === 'docx') {
    return {
      fileName: file.name,
      extension,
      columns: [],
      rows: [],
      sheets: [],
      warnings: ['Документ загружен. Таблицу из PDF/DOCX прочитаем на backend при генерации.'],
    };
  }

  return {
    fileName: file.name,
    extension,
    columns: [],
    rows: [],
    sheets: [],
    warnings: ['Поддерживаются CSV, XLSX, XLS, PDF и DOCX.'],
  };
}

export function Workspace({ profile, history, onLogout, onProfileUpdate, onSaveHistory }: Props) {
  const [schema, setSchema] = useState(defaultSchema);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [parsedFile, setParsedFile] = useState<ParsedFileInfo | null>(null);
  const [result, setResult] = useState<GenerationResult>({ code: defaultCode, mappings: [], preview: [], warnings: [] });
  const [busy, setBusy] = useState(false);
  const [activeHistoryId, setActiveHistoryId] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [copied, setCopied] = useState(false);
  const [activePreviewSheet, setActivePreviewSheet] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<'generator' | 'profile'>('generator');
  const [displayName, setDisplayName] = useState(profile.name);
  const [profileSaveBusy, setProfileSaveBusy] = useState(false);
  const [profileSaveNotice, setProfileSaveNotice] = useState('');
  const [profileSaveError, setProfileSaveError] = useState('');
  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [accountModalMode, setAccountModalMode] = useState<'email' | 'password'>('email');
  const [newEmail, setNewEmail] = useState('');
  const [emailPassword, setEmailPassword] = useState('');
  const [emailChangeCode, setEmailChangeCode] = useState('');
  const [emailChangeBusy, setEmailChangeBusy] = useState(false);
  const [emailCodeBusy, setEmailCodeBusy] = useState(false);
  const [emailChangeNotice, setEmailChangeNotice] = useState('');
  const [emailChangeError, setEmailChangeError] = useState('');
  const [emailCodeRequested, setEmailCodeRequested] = useState(false);
  const [currentPassword, setCurrentPassword] = useState('');
  const [nextPassword, setNextPassword] = useState('');
  const [repeatPassword, setRepeatPassword] = useState('');
  const [passwordChangeBusy, setPasswordChangeBusy] = useState(false);
  const [passwordChangeNotice, setPasswordChangeNotice] = useState('');
  const [passwordChangeError, setPasswordChangeError] = useState('');
  const hasGeneratedResult = result.code !== defaultCode;

  const previewSheets = useMemo(() => {
    if (!parsedFile) {
      return [];
    }

    if (parsedFile.sheets.length > 0) {
      return parsedFile.sheets;
    }

    if (parsedFile.columns.length === 0 && parsedFile.rows.length === 0) {
      return [];
    }

    return [buildPreviewSheet(parsedFile.fileName, parsedFile.columns, parsedFile.rows)];
  }, [parsedFile]);

  const currentPreviewSheet = useMemo(() => {
    if (previewSheets.length === 0) {
      return null;
    }

    return previewSheets.find((sheet) => sheet.name === activePreviewSheet) ?? previewSheets[0];
  }, [activePreviewSheet, previewSheets]);

  const fileSummary = useMemo(() => {
    if (!parsedFile) {
      return 'Файл еще не загружен';
    }

    if (parsedFile.extension === 'pdf' || parsedFile.extension === 'docx') {
      return `${parsedFile.fileName} · документ загружен`;
    }

    if (parsedFile.sheets.length > 1) {
      return `${parsedFile.fileName} · ${parsedFile.sheets.length} sheets · ${parsedFile.rows.length} preview rows`;
    }

    return `${parsedFile.fileName} · ${parsedFile.columns.length} колонок · ${parsedFile.rows.length} preview rows`;
  }, [parsedFile]);

  const visibleWarnings = useMemo(() => {
    return Array.from(new Set([...result.warnings, ...(parsedFile?.warnings ?? []), saveMessage].filter(Boolean)));
  }, [parsedFile?.warnings, result.warnings, saveMessage]);

  const profileStats = useMemo(() => {
    const totalGenerations = history.length;
    const uniqueFiles = new Set(history.map((item) => item.fileName)).size;
    const totalWarnings = history.reduce((sum, item) => sum + item.warnings.length, 0);
    const lastGeneratedAt = history[0]?.createdAt ?? null;
    return {
      totalGenerations,
      uniqueFiles,
      totalWarnings,
      lastGeneratedAt,
    };
  }, [history]);

  const recentFiles = useMemo(() => {
    const seen = new Set<string>();
    return history
      .filter((item) => {
        if (seen.has(item.fileName)) {
          return false;
        }
        seen.add(item.fileName);
        return true;
      })
      .slice(0, 6)
      .map((item) => ({
        id: item.id,
        fileName: item.fileName,
        createdAt: item.createdAt,
      }));
  }, [history]);

  const frequentSchemas = useMemo(() => {
    const schemaMap = new Map<
      string,
      {
        schema: string;
        count: number;
        lastUsedAt: string;
        sampleFileName: string;
      }
    >();

    history.forEach((item) => {
      const existing = schemaMap.get(item.schema);
      if (existing) {
        existing.count += 1;
        if (new Date(item.createdAt).getTime() > new Date(existing.lastUsedAt).getTime()) {
          existing.lastUsedAt = item.createdAt;
          existing.sampleFileName = item.fileName;
        }
        return;
      }

      schemaMap.set(item.schema, {
        schema: item.schema,
        count: 1,
        lastUsedAt: item.createdAt,
        sampleFileName: item.fileName,
      });
    });

    return Array.from(schemaMap.values())
      .sort((a, b) => {
        if (b.count !== a.count) {
          return b.count - a.count;
        }
        return new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime();
      })
      .slice(0, 5)
      .map((item) => {
        let label = 'JSON шаблон';
        try {
          const parsed = JSON.parse(item.schema) as Record<string, unknown>;
          const keys = Object.keys(parsed);
          if (keys.length > 0) {
            label = keys.slice(0, 3).join(', ');
            if (keys.length > 3) {
              label += ` +${keys.length - 3}`;
            }
          }
        } catch {
          label = item.sampleFileName;
        }

        return {
          ...item,
          label,
        };
      });
  }, [history]);

  const restoreHistoryItem = (item: HistoryItem) => {
    setActiveHistoryId(item.id);
    setSchema(item.schema);
    setResult({
      code: item.code,
      mappings: item.mappings,
      preview: item.preview,
      warnings: item.warnings,
    });
    setActiveView('generator');
  };

  useEffect(() => {
    if (previewSheets.length === 0) {
      if (activePreviewSheet !== null) {
        setActivePreviewSheet(null);
      }
      return;
    }

    if (!activePreviewSheet || !previewSheets.some((sheet) => sheet.name === activePreviewSheet)) {
      setActivePreviewSheet(previewSheets[0].name);
    }
  }, [activePreviewSheet, previewSheets]);

  useEffect(() => {
    setDisplayName(profile.name);
  }, [profile.name]);

  const handleSelectedFile = async (file: File) => {
    setSelectedFile(file);
    const parsed = await parseFile(file);
    setParsedFile(parsed);
    setActivePreviewSheet(parsed.sheets[0]?.name ?? null);
    setSaveMessage('');
  };

  const onFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    await handleSelectedFile(file);
  };

  const onDragEnter = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragActive(true);
  };

  const onDragOver = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    if (!dragActive) {
      setDragActive(true);
    }
  };

  const onDragLeave = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    if (event.currentTarget.contains(event.relatedTarget as Node | null)) return;
    setDragActive(false);
  };

  const onDrop = async (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (!file) return;
    await handleSelectedFile(file);
  };

  const onGenerate = async () => {
    if (!selectedFile) {
      setResult({
        code: defaultCode,
        mappings: [],
        preview: [],
        warnings: ['Сначала загрузите CSV, XLSX, XLS, PDF или DOCX.'],
      });
      return;
    }

    setBusy(true);
    try {
      const generated = await generateFromBackend({
        file: selectedFile,
        targetJson: schema,
        userId: profile.skipped ? undefined : profile.id,
        selectedSheet:
          parsedFile?.extension === 'xlsx' || parsedFile?.extension === 'xls'
            ? currentPreviewSheet?.name
            : undefined,
      });

      setParsedFile(generated.parsedFile ?? parsedFile);
      setResult(generated);
      setSaveMessage('');

      if (!profile.skipped) {
        if (generated.generationId) {
          setActiveHistoryId(generated.generationId);
        }
        try {
          await onSaveHistory();
        } catch (historyError) {
          setSaveMessage(
            historyError instanceof Error
              ? historyError.message
              : 'Generation finished, but the history list could not be refreshed.'
          );
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось выполнить генерацию.';
      setResult({
        code: defaultCode,
        mappings: [],
        preview: [],
        warnings: [message],
      });
    } finally {
      setBusy(false);
    }
  };

  const onDownload = async () => {
    if (window.electronAPI) {
      const saved = await window.electronAPI.saveGeneratedFile({
        code: result.code,
        suggestedName: `${parsedFile?.fileName?.split('.')?.[0] ?? 'parser'}.ts`,
      });
      if (!saved.canceled && saved.filePath) {
        setSaveMessage(`Файл сохранен: ${saved.filePath}`);
      }
      return;
    }

    const blob = new Blob([result.code], { type: 'text/typescript;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'parser.ts';
    anchor.click();
    URL.revokeObjectURL(url);
    setSaveMessage('Файл скачан через браузер.');
  };

  const onCopyCode = async () => {
    if (!result.code) return;
    try {
      await navigator.clipboard.writeText(result.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch (error) {
      console.error(error);
    }
  };

  const resetEmailChangeState = () => {
    setNewEmail('');
    setEmailPassword('');
    setEmailChangeCode('');
    setEmailChangeError('');
    setEmailChangeNotice('');
    setEmailCodeRequested(false);
  };

  const resetPasswordChangeState = () => {
    setCurrentPassword('');
    setNextPassword('');
    setRepeatPassword('');
    setPasswordChangeError('');
    setPasswordChangeNotice('');
  };

  const closeAccountModal = () => {
    setAccountModalOpen(false);
    resetEmailChangeState();
    resetPasswordChangeState();
  };

  const sendEmailChangeCode = async () => {
    if (!newEmail.trim()) {
      setEmailChangeError('Введите новый email.');
      return;
    }

    setEmailCodeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const response = await requestEmailChangeCode({ userId: profile.id, newEmail: newEmail.trim().toLowerCase() });
      const ttlMinutes = Math.max(1, Math.ceil(response.expiresIn / 60));
      setEmailCodeRequested(true);
      setEmailChangeNotice(`${response.message} Код действует ${ttlMinutes} мин.`);
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось отправить код.');
    } finally {
      setEmailCodeBusy(false);
    }
  };

  const submitEmailChangeByPassword = async () => {
    setEmailChangeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const nextProfile = await changeEmailWithPassword({
        userId: profile.id,
        newEmail: newEmail.trim().toLowerCase(),
        currentPassword: emailPassword,
      });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      resetEmailChangeState();
      setEmailChangeNotice('Почта успешно обновлена через пароль.');
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось сменить почту.');
    } finally {
      setEmailChangeBusy(false);
    }
  };

  const submitEmailChangeByCode = async () => {
    setEmailChangeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const nextProfile = await changeEmailWithCode({
        userId: profile.id,
        newEmail: newEmail.trim().toLowerCase(),
        verificationCode: emailChangeCode.trim(),
      });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      resetEmailChangeState();
      setEmailChangeNotice('Почта успешно обновлена по коду из письма.');
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось сменить почту.');
    } finally {
      setEmailChangeBusy(false);
    }
  };

  const submitDisplayName = async () => {
    if (profile.skipped) {
      return;
    }

    setProfileSaveBusy(true);
    setProfileSaveError('');
    setProfileSaveNotice('');

    try {
      const nextProfile = await updateProfileName({ userId: profile.id, name: displayName.trim() });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      setProfileSaveNotice('Имя обновлено.');
    } catch (error) {
      setProfileSaveError(error instanceof Error ? error.message : 'Не удалось обновить имя.');
    } finally {
      setProfileSaveBusy(false);
    }
  };

  const submitPasswordChange = async () => {
    if (!currentPassword.trim()) {
      setPasswordChangeError('Введите текущий пароль.');
      return;
    }
    if (!nextPassword.trim()) {
      setPasswordChangeError('Введите новый пароль.');
      return;
    }
    if (nextPassword.trim().length < 8) {
      setPasswordChangeError('Пароль должен содержать минимум 8 символов.');
      return;
    }
    if (nextPassword !== repeatPassword) {
      setPasswordChangeError('Новый пароль и повтор не совпадают.');
      return;
    }

    setPasswordChangeBusy(true);
    setPasswordChangeError('');
    setPasswordChangeNotice('');

    try {
      const message = await changePasswordWithBackend({
        userId: profile.id,
        currentPassword,
        newPassword: nextPassword,
      });
      resetPasswordChangeState();
      setPasswordChangeNotice(message);
    } catch (error) {
      setPasswordChangeError(error instanceof Error ? error.message : 'Не удалось сменить пароль.');
    } finally {
      setPasswordChangeBusy(false);
    }
  };

  return (
    <div
      className="workspace-stage"
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      onDragOver={onDragOver}
      onDrop={onDrop}
    >
      <VibeBackground className="workspace-scene" baseScale={1.08} energy={0.26} lite staticFrame />
      <div className="workspace-overlay" />

      <div className="workspace-shell">
        <aside className="sidebar glass-card">
          <div className="sidebar-top">
            <div>
              <div className="eyebrow">Workspace</div>
              <h2>TSGen</h2>
              <p className="subtle-text">{activeView === 'generator' ? 'Генерация и просмотр результата.' : 'Профиль и настройки аккаунта.'}</p>
            </div>
            <button className="icon-btn" onClick={onLogout} title="Выйти" type="button">
              <LogOut size={16} />
            </button>
          </div>

          <div className="mode-switch workspace-mode-switch">
            <button className={activeView === 'generator' ? 'active' : ''} onClick={() => setActiveView('generator')} type="button">
              Генерация
            </button>
            <button className={activeView === 'profile' ? 'active' : ''} onClick={() => setActiveView('profile')} type="button">
              Профиль
            </button>
          </div>

          {activeView === 'generator' ? (
            <section className="generator-panel">
              <div className="panel-title">
                <Sparkles size={16} /> Генерация
              </div>

              <label className={dragActive ? 'upload-zone drag-active' : 'upload-zone'}>
                <input accept=".csv,.xlsx,.xls,.pdf,.docx" hidden onChange={onFileChange} type="file" />
                <Upload size={18} />
                <strong>Загрузить CSV/XLSX/PDF/DOCX</strong>
                <span>{fileSummary}</span>
              </label>

              <div className="field-block">
                <div className="field-caption">Target JSON</div>
                <textarea className="editor-area" onChange={(event) => setSchema(event.target.value)} value={schema} />
              </div>

              <button className="primary-btn" disabled={busy} onClick={onGenerate} type="button">
                <WandSparkles size={16} /> {busy ? 'Генерируем...' : 'Сгенерировать'}
              </button>

              <button
                className={hasGeneratedResult ? 'download-btn ready' : 'download-btn'}
                disabled={!hasGeneratedResult}
                onClick={onDownload}
                type="button"
              >
                <Download size={16} /> Скачать .ts
              </button>
            </section>
          ) : (
            <section className="profile-nav-card sidebar-history-card">
              <div className="panel-title">
                <History size={16} /> История генераций
              </div>
              <div className="history-list sidebar-history-list">
                {history.length === 0 && <div className="empty-card compact">Пока пусто.</div>}
                {history.map((item) => (
                  <button
                    className={item.id === activeHistoryId ? 'history-item active' : 'history-item'}
                    key={item.id}
                    onClick={() => restoreHistoryItem(item)}
                    type="button"
                  >
                    <strong>{item.fileName}</strong>
                    <span>{new Date(item.createdAt).toLocaleString()}</span>
                  </button>
                ))}
              </div>
            </section>
          )}
        </aside>

        <main className="viewer-area glass-card">
          {activeView === 'generator' ? (
            <>
              <div className="viewer-toolbar">
                <div>
                  <div className="eyebrow">Generated output</div>
                  <h2>Код и просмотр результата</h2>
                </div>
              </div>

              <div className="viewer-grid">
                <section className="viewer-pane">
                  <div className="pane-header">
                    <FileSpreadsheet size={16} /> Preview файла
                  </div>
                  {previewSheets.length > 1 && (
                    <div className="sheet-tab-row">
                      {previewSheets.map((sheet) => (
                        <button
                          className={sheet.name === currentPreviewSheet?.name ? 'sheet-tab active' : 'sheet-tab'}
                          key={sheet.name}
                          onClick={() => setActivePreviewSheet(sheet.name)}
                          type="button"
                        >
                          <span>{sheet.name}</span>
                          <small>{sheet.rows.length} rows</small>
                        </button>
                      ))}
                    </div>
                  )}
                  <div className="data-grid-wrap">
                    {currentPreviewSheet && (currentPreviewSheet.columns.length > 0 || currentPreviewSheet.rows.length > 0) ? (
                      <table className="data-grid">
                        <thead>
                          <tr>
                            {currentPreviewSheet.columns.map((column) => (
                              <th key={column}>{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {currentPreviewSheet.rows.map((row, index) => (
                            <tr key={index}>
                              {currentPreviewSheet.columns.map((column) => (
                                <td key={`${index}-${column}`}>{String(row[column] ?? '')}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      <div className="empty-card">После загрузки тут покажется содержимое файла.</div>
                    )}
                  </div>
                </section>

                <section className="viewer-pane">
                  <div className="pane-header pane-header-with-action">
                    <span className="pane-header-label">
                      <Sparkles size={16} /> Generated TypeScript
                    </span>
                    <button className="icon-btn copy-code-btn" onClick={onCopyCode} title="Скопировать код" type="button">
                      {copied ? <Check size={16} /> : <Copy size={16} />}
                    </button>
                  </div>
                  <pre className="code-pane">{result.code}</pre>
                </section>
              </div>

              <div className="insight-grid">
                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Preview JSON
                  </div>
                  <pre className="preview-pane">{JSON.stringify(result.preview, null, 2)}</pre>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <TriangleAlert size={16} /> Warnings
                  </div>
                  <div className="warning-list">
                    {visibleWarnings.map((warning, index) => (
                      <div className="warning-item" key={index}>
                        {warning}
                      </div>
                    ))}
                    {visibleWarnings.length === 0 && <div className="empty-card compact">Пока без предупреждений.</div>}
                  </div>
                </section>
              </div>
            </>
          ) : (
            <>
              <div className="viewer-toolbar">
                <div>
                  <div className="eyebrow">Account</div>
                  <h2>Профиль пользователя</h2>
                </div>
              </div>

              <div className="profile-view-grid">
                <section className="insight-card">
                  <div className="pane-header">
                    <UserRound size={16} /> Аккаунт
                  </div>
                  <div className="profile-details">
                    <div className="profile-detail">
                      <span>Имя</span>
                      <div className="profile-inline-form">
                        <div className="auth-input-wrap">
                          <UserRound size={18} />
                          <input
                            placeholder="Введите имя"
                            type="text"
                            value={displayName}
                            onChange={(event) => setDisplayName(event.target.value)}
                          />
                        </div>
                        <button className="secondary-btn profile-inline-save" disabled={profileSaveBusy || profile.skipped} onClick={submitDisplayName} type="button">
                          {profileSaveBusy ? 'Сохраняем...' : 'Сохранить имя'}
                        </button>
                      </div>
                    </div>
                    <div className="profile-detail">
                      <span>Почта</span>
                      <div className="profile-detail-row">
                        <strong>{profile.email}</strong>
                        {!profile.skipped && (
                          <button
                            className="icon-btn profile-edit-btn"
                            onClick={() => {
                              setAccountModalMode('email');
                              setAccountModalOpen(true);
                            }}
                            title="Сменить почту"
                            type="button"
                          >
                            <SquarePen size={16} />
                          </button>
                        )}
                      </div>
                    </div>
                    <div className="profile-detail">
                      <span>Пароль</span>
                      <div className="profile-detail-row">
                        <strong>••••••••••</strong>
                        {!profile.skipped && (
                          <button
                            className="icon-btn profile-edit-btn"
                            onClick={() => {
                              setAccountModalMode('password');
                              setAccountModalOpen(true);
                            }}
                            title="Сменить пароль"
                            type="button"
                          >
                            <SquarePen size={16} />
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                  {profileSaveNotice && <div className="auth-status auth-status-success">{profileSaveNotice}</div>}
                  {profileSaveError && <div className="warning-item auth-status auth-status-error">{profileSaveError}</div>}
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Статистика аккаунта
                  </div>
                  <div className="profile-fun-grid">
                    <div className="empty-card compact">
                      <strong>{profileStats.totalGenerations}</strong>
                      <span>генераций</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.uniqueFiles}</strong>
                      <span>уникальных файлов</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.totalWarnings}</strong>
                      <span>предупреждений всего</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.lastGeneratedAt ? new Date(profileStats.lastGeneratedAt).toLocaleDateString() : '—'}</strong>
                      <span>последняя генерация</span>
                    </div>
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <FileSpreadsheet size={16} /> Последние файлы
                  </div>
                  <div className="profile-list">
                    {recentFiles.length === 0 && <div className="empty-card compact">Пока нет загруженных файлов.</div>}
                    {recentFiles.map((item) => (
                      <div className="profile-list-item" key={item.id}>
                        <div>
                          <strong>{item.fileName}</strong>
                          <span>{new Date(item.createdAt).toLocaleString()}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Часто используемые JSON
                  </div>
                  <div className="profile-list">
                    {frequentSchemas.length === 0 && <div className="empty-card compact">Пока нет сохранённых шаблонов.</div>}
                    {frequentSchemas.map((item) => (
                      <button
                        className="profile-list-item profile-list-item-button"
                        key={`${item.label}-${item.lastUsedAt}`}
                        onClick={() => {
                          setSchema(item.schema);
                          setActiveView('generator');
                        }}
                        type="button"
                      >
                        <div>
                          <strong>{item.label}</strong>
                          <span>Использован {item.count} раз(а) · последний файл {item.sampleFileName}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </section>
              </div>
            </>
          )}
        </main>
      </div>

      {accountModalOpen && !profile.skipped && (
        <div className="profile-modal-backdrop" role="presentation" onClick={closeAccountModal}>
          <section className="profile-modal glass-card" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="profile-modal-header">
              <div>
                <div className="eyebrow">Account edit</div>
                <h3>{accountModalMode === 'email' ? 'Сменить почту' : 'Сменить пароль'}</h3>
              </div>
              <button className="icon-btn" onClick={closeAccountModal} title="Закрыть" type="button">
                <X size={16} />
              </button>
            </div>

            <div className="mode-switch workspace-mode-switch profile-modal-switch">
              <button className={accountModalMode === 'email' ? 'active' : ''} onClick={() => setAccountModalMode('email')} type="button">
                Почта
              </button>
              <button className={accountModalMode === 'password' ? 'active' : ''} onClick={() => setAccountModalMode('password')} type="button">
                Пароль
              </button>
            </div>

            {accountModalMode === 'email' ? (
              <div className="profile-email-form">
                <label className="auth-field">
                  <span className="auth-field-label">Новый email</span>
                  <div className="auth-input-wrap">
                    <Mail size={18} />
                    <input
                      placeholder="new@email.com"
                      type="email"
                      value={newEmail}
                      onChange={(event) => {
                        setNewEmail(event.target.value);
                        setEmailChangeCode('');
                        setEmailCodeRequested(false);
                      }}
                    />
                  </div>
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Через текущий пароль</span>
                  <PasswordInput icon={LockKeyhole} onChange={setEmailPassword} placeholder="Введите текущий пароль" value={emailPassword} />
                </label>

                <button className="primary-btn" disabled={emailChangeBusy} onClick={submitEmailChangeByPassword} type="button">
                  <ShieldCheck size={16} /> Сменить почту через пароль
                </button>

                <div className="profile-divider" />

                <div className="auth-code-inline">
                  <div className="auth-input-wrap auth-code-input">
                    <ShieldCheck size={18} />
                    <input
                      placeholder="Код с текущей почты"
                      type="text"
                      value={emailChangeCode}
                      onChange={(event) => setEmailChangeCode(event.target.value)}
                    />
                  </div>
                  <button className="secondary-btn auth-code-btn auth-code-btn-inline" disabled={emailCodeBusy} onClick={sendEmailChangeCode} type="button">
                    <Mail size={16} />
                    <span>{emailCodeBusy ? 'Отправляем...' : 'Получить код'}</span>
                  </button>
                </div>

                <button className="primary-btn" disabled={emailChangeBusy || !emailCodeRequested} onClick={submitEmailChangeByCode} type="button">
                  <ShieldCheck size={16} /> Сменить почту по коду
                </button>

                {emailChangeNotice && <div className="auth-status auth-status-success">{emailChangeNotice}</div>}
                {emailChangeError && <div className="warning-item auth-status auth-status-error">{emailChangeError}</div>}
              </div>
            ) : (
              <div className="profile-email-form">
                <label className="auth-field">
                  <span className="auth-field-label">Текущий пароль</span>
                  <PasswordInput icon={LockKeyhole} onChange={setCurrentPassword} placeholder="Введите текущий пароль" value={currentPassword} />
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Новый пароль</span>
                  <PasswordInput icon={KeyRound} onChange={setNextPassword} placeholder="Минимум 8 символов" value={nextPassword} />
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Повторите пароль</span>
                  <PasswordInput icon={ShieldCheck} onChange={setRepeatPassword} placeholder="Повторите новый пароль" value={repeatPassword} />
                </label>

                <button className="primary-btn" disabled={passwordChangeBusy} onClick={submitPasswordChange} type="button">
                  <ShieldCheck size={16} /> {passwordChangeBusy ? 'Сохраняем...' : 'Сменить пароль'}
                </button>

                {passwordChangeNotice && <div className="auth-status auth-status-success">{passwordChangeNotice}</div>}
                {passwordChangeError && <div className="warning-item auth-status auth-status-error">{passwordChangeError}</div>}
              </div>
            )}
          </section>
        </div>
      )}
    </div>
  );
}
