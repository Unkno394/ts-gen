const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  saveGeneratedFile: (payload) => ipcRenderer.invoke('save-generated-file', payload),
  backendBaseUrl: process.env.BACKEND_URL || ''
});
