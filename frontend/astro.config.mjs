import { defineConfig } from 'astro/config';
import node from '@astrojs/node';
import react from '@astrojs/react';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  output: 'server',
  adapter: node({
    mode: 'standalone',
  }),
  integrations: [react(), tailwind()],
  server: {
    port: parseInt(process.env.FRONTEND_PORT || '4321'),
    host: '0.0.0.0',
  },
  vite: {
    server: {
      proxy: {
        '/api': {
          target: process.env.BACKEND_URL || 'http://localhost:8180',
          changeOrigin: true,
        },
      },
    },
  },
});
