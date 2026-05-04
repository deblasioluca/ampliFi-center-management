import { defineConfig } from 'astro/config';
import react from '@astrojs/react';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  output: 'static',
  integrations: [react(), tailwind()],
  server: {
    port: parseInt(process.env.FRONTEND_PORT || '4321'),
    host: '0.0.0.0',
  },
  vite: {
    server: {
      allowedHosts: true,
    },
  },
});
