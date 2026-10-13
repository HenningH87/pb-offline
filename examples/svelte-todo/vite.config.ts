import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// This example imports the library the way any consumer would: by its
// package name, resolved here to the repo's own built dist/ instead of a
// published tarball. Nothing under src/ is reachable from this app — run
// `npm run build` at the repo root before `npm run dev` here (SPEC §17).
export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: {
      'pb-offline': fileURLToPath(new URL('../../dist/index.js', import.meta.url)),
    },
  },
});
