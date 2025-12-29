import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  base: '/dbsp.js/', // GitHub Pages base path - matches repo name
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@dbsp': path.resolve(__dirname, '../src'), // Points to package source
    },
  },
  build: {
    outDir: 'dist',
  },
});

