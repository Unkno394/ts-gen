export type AuthMode = 'register' | 'login';

export type UserProfile = {
  id: string;
  name: string;
  email: string;
  skipped?: boolean;
};

export type HistoryItem = {
  id: string;
  createdAt: string;
  fileName: string;
  schema: string;
  code: string;
  mappings: MappingInfo[];
  preview: Record<string, unknown>[];
  warnings: string[];
};

export type ParsedFileInfo = {
  fileName: string;
  extension: string;
  columns: string[];
  rows: Record<string, unknown>[];
  sheets: ParsedSheetInfo[];
  warnings: string[];
};

export type ParsedSheetInfo = {
  name: string;
  columns: string[];
  rows: Record<string, unknown>[];
};

export type MappingInfo = {
  source: string;
  target: string;
  confidence: 'high' | 'medium' | 'low' | 'none';
  reason?: string;
};

export type GenerationResult = {
  generationId?: string | null;
  parsedFile?: ParsedFileInfo | null;
  code: string;
  mappings: MappingInfo[];
  preview: Record<string, unknown>[];
  warnings: string[];
};
