/// <reference types="vite/client" />

declare global {
  interface Window {
    electronAPI?: {
      saveGeneratedFile: (payload: { code: string; suggestedName?: string }) => Promise<{ canceled: boolean; filePath?: string }>;
      backendBaseUrl?: string;
    };
  }
}

export {};
