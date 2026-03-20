import {
  Check,
  Copy,
  Download,
  FileSpreadsheet,
  History,
  Info,
  LockKeyhole,
  LogOut,
  Sparkles,
  TriangleAlert,
  Upload,
  WandSparkles,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import * as XLSX from 'xlsx';
import { generateFromBackend } from '../lib/api';
import type { GenerationResult, HistoryItem, ParsedFileInfo, ParsedSheetInfo, UserProfile } from '../types';
import { VibeBackground } from './VibeBackground';

type Props = {
  profile: UserProfile;
  history: HistoryItem[];
  onLogout: () => void;
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

export function Workspace({ profile, history, onLogout, onSaveHistory }: Props) {
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

  const isGuest = Boolean(profile.skipped);
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
        userId: isGuest ? undefined : profile.id,
        selectedSheet:
          parsedFile?.extension === 'xlsx' || parsedFile?.extension === 'xls'
            ? currentPreviewSheet?.name
            : undefined,
      });

      setParsedFile(generated.parsedFile ?? parsedFile);
      setResult(generated);
      setSaveMessage('');

      if (!isGuest) {
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

  return (
    <div
      className="workspace-stage"
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      onDragOver={onDragOver}
      onDrop={onDrop}
    >
      <VibeBackground className="workspace-scene" baseScale={1.08} energy={0.26} lite />
      <div className="workspace-overlay" />

      <div className="workspace-shell">
        <aside className="sidebar glass-card">
          <div className="sidebar-top">
            <div>
              <div className="eyebrow">Session</div>
              <h2>{isGuest ? 'Guest mode' : profile.name}</h2>
              <p className="subtle-text">{isGuest ? 'История генераций не сохраняется.' : profile.email}</p>
            </div>
            <button className="icon-btn" onClick={onLogout} title="Выйти" type="button">
              <LogOut size={16} />
            </button>
          </div>

          {isGuest && (
            <button className="secondary-btn guest-auth-btn" onClick={onLogout} type="button">
              <LockKeyhole size={16} /> Войти / зарегистрироваться
            </button>
          )}

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

          {!isGuest && (
            <section className="history-panel">
              <div className="panel-title">
                <History size={16} /> История генераций
              </div>
              <div className="history-list">
                {history.length === 0 && <div className="empty-card">Пока пусто. Первая генерация появится здесь.</div>}
                {history.map((item) => (
                  <button
                    className={item.id === activeHistoryId ? 'history-item active' : 'history-item'}
                    key={item.id}
                    onClick={() => {
                      setActiveHistoryId(item.id);
                      setSchema(item.schema);
                      setResult({
                        code: item.code,
                        mappings: item.mappings,
                        preview: item.preview,
                        warnings: item.warnings,
                      });
                    }}
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
            {!isGuest && (
              <section className="insight-card">
                <div className="pane-header">
                  <Info size={16} /> Mapping
                </div>
                {result.mappings.length === 0 ? (
                  <div className="empty-card compact">После генерации тут будут сопоставления полей.</div>
                ) : (
                  <div className="mapping-list">
                    {result.mappings.map((mapping) => (
                      <div className="mapping-item" key={mapping.target}>
                        <div>
                          <strong>{mapping.target}</strong>
                          <span>{mapping.source}</span>
                        </div>
                        <span className={`confidence ${mapping.confidence}`}>{mapping.confidence}</span>
                      </div>
                    ))}
                  </div>
                )}
              </section>
            )}

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
        </main>
      </div>
    </div>
  );
}
