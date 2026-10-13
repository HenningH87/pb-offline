import PocketBase from 'pocketbase';
// Imported by package name, resolved to the repo's built dist/ by the alias
// in vite.config.ts — this is exactly what a real consumer would write.
import { createPBOffline } from 'pb-offline';
import type { Todo } from './types';

export const pb = new PocketBase(import.meta.env.VITE_PB_URL ?? 'http://127.0.0.1:8090');

export const pbo = createPBOffline(pb, {
  appId: 'pb-offline-todo-example',
  collections: [{ name: 'todos' }],
});

export const todos = pbo.collection<Todo>('todos');

// start() is the only async step (SPEC §2.1) — everything above it is
// synchronous and every store above is already emitting.
void pbo.start();
