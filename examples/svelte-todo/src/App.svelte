<script lang="ts">
  import { onMount } from 'svelte';
  import { pb, pbo, todos } from './lib/db';

  // ---- auth ---------------------------------------------------------
  let loggedIn = $state(pb.authStore.isValid);
  let email = $state('');
  let password = $state('');
  let authError = $state('');
  let authBusy = $state(false);

  onMount(() =>
    pb.authStore.onChange(() => {
      loggedIn = pb.authStore.isValid;
    }),
  );

  async function login(e: SubmitEvent) {
    e.preventDefault();
    authError = '';
    authBusy = true;
    try {
      await pb.authWithPassword(email, password);
      password = '';
    } catch (err) {
      authError = err instanceof Error ? err.message : 'Could not log in.';
    } finally {
      authBusy = false;
    }
  }

  function logout() {
    pb.authStore.clear();
  }

  // ---- anonymous -> owned promotion ----------------------------------
  let promoteMessage = $state('');

  async function bringOfflineTodos() {
    const result = await pbo.promoteAnonymousData();
    promoteMessage =
      result.promoted === 0
        ? 'No offline todos were waiting.'
        : `Brought ${result.promoted} offline todo${result.promoted === 1 ? '' : 's'} into this account.`;
  }

  // ---- todos -----------------------------------------------------------
  const list = todos.live({ sort: { field: 'created', dir: 'desc' } });

  let newTodoText = $state('');

  async function addTodo(e: SubmitEvent) {
    e.preventDefault();
    const text = newTodoText.trim();
    if (!text) return;
    newTodoText = '';
    await todos.create({ text, done: false, owner: pb.authStore.record?.id ?? '' });
  }

  async function toggle(id: string, done: boolean) {
    await todos.update(id, { done: !done });
  }

  async function remove(id: string) {
    await todos.delete(id);
  }

  // A write the server's API rules reject (empty text), so the dead-letter
  // drawer below has something to show. Nothing else in this app can fail
  // this way — it exists purely to demonstrate §5.6 of SPEC.md.
  async function triggerRejectedWrite() {
    await todos.create({ text: '', done: false, owner: pb.authStore.record?.id ?? '' });
  }

  // ---- sync state --------------------------------------------------
  const status = pbo.status;
  const pendingCount = pbo.pendingCount;
  const online = pbo.online;
  const lastSyncedAt = pbo.lastSyncedAt;
  const deadLetters = pbo.deadLetters;

  function formatTime(ms: number | null) {
    return ms === null ? 'never' : new Date(ms).toLocaleTimeString();
  }
</script>

<main>
  <h1>todos <span class="brand">— pb-offline example</span></h1>

  <section class="status-bar" class:offline={!$online}>
    <span class="dot" class:up={$online}></span>
    <strong>{$status}</strong>
    <span>· {$pendingCount} pending</span>
    <span>· last synced {formatTime($lastSyncedAt)}</span>
  </section>

  {#if !loggedIn}
    <section class="panel">
      <p class="hint">
        Not signed in. Todos you add now are stored locally under an anonymous
        profile and are never sent to the server — that's <code>anonymousMode: 'local'</code>
        from SPEC §14.3, not a bug.
      </p>
      <form onsubmit={login}>
        <input type="email" placeholder="email" bind:value={email} required />
        <input type="password" placeholder="password" bind:value={password} required />
        <button type="submit" disabled={authBusy}>Log in</button>
      </form>
      {#if authError}<p class="error">{authError}</p>{/if}
    </section>
  {:else}
    <section class="panel">
      <p class="hint">Signed in as <strong>{pb.authStore.record?.email}</strong></p>
      <button onclick={bringOfflineTodos}>Bring my offline todos</button>
      <button onclick={logout}>Log out</button>
      {#if promoteMessage}<p class="hint">{promoteMessage}</p>{/if}
    </section>
  {/if}

  <form onsubmit={addTodo} class="add-form">
    <input type="text" placeholder="What needs doing?" bind:value={newTodoText} />
    <button type="submit">Add</button>
  </form>

  <ul class="todos">
    {#each $list as todo (todo.id)}
      <li class:done={todo.done}>
        <input type="checkbox" checked={todo.done} onchange={() => toggle(todo.id, todo.done)} />
        <span class="text">{todo.text}</span>
        <button class="ghost" onclick={() => remove(todo.id)}>delete</button>
      </li>
    {:else}
      <li class="empty">No todos yet.</li>
    {/each}
  </ul>

  <p><button class="ghost" onclick={triggerRejectedWrite}>Simulate a rejected write</button></p>

  {#if $deadLetters.length > 0}
    <section class="deadletters">
      <h2>Dead letters ({$deadLetters.length})</h2>
      <p class="hint">
        These writes were rejected by the server and left exactly as you made
        them — nothing is ever silently dropped or reverted.
      </p>
      <ul>
        {#each $deadLetters as entry (entry.id)}
          <li>
            <code>{entry.op}</code> {entry.collection}/{entry.recordId} —
            <span class="error">{entry.error.message}</span>
            <button onclick={() => pbo.retryDeadLetter(entry.id)}>Retry</button>
            <button class="ghost" onclick={() => pbo.discardDeadLetter(entry.id)}>Discard</button>
          </li>
        {/each}
      </ul>
    </section>
  {/if}
</main>

<style>
  :global(body) {
    margin: 0;
    background: #fafaf9;
    color: #1c1c1a;
    font-family:
      system-ui,
      -apple-system,
      'Segoe UI',
      sans-serif;
  }

  main {
    max-width: 32rem;
    margin: 0 auto;
    padding: 2rem 1.25rem 4rem;
  }

  h1 {
    font-size: 1.25rem;
    margin: 0 0 1rem;
  }

  .brand {
    font-weight: 400;
    color: #78716c;
  }

  .status-bar {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.85rem;
    padding: 0.5rem 0.75rem;
    border-radius: 0.5rem;
    background: #eef2ee;
    color: #2b4a34;
    margin-bottom: 1.25rem;
  }

  .status-bar.offline {
    background: #f5efe6;
    color: #7a5b1e;
  }

  .dot {
    width: 0.5rem;
    height: 0.5rem;
    border-radius: 999px;
    background: #b45309;
  }

  .dot.up {
    background: #2f855a;
  }

  .panel {
    border: 1px solid #e7e5e4;
    border-radius: 0.5rem;
    padding: 0.75rem 1rem;
    margin-bottom: 1.25rem;
  }

  .hint {
    font-size: 0.85rem;
    color: #57534e;
    margin: 0.25rem 0;
  }

  form {
    display: flex;
    gap: 0.5rem;
  }

  .add-form {
    margin-bottom: 1rem;
  }

  input[type='text'],
  input[type='email'],
  input[type='password'] {
    flex: 1;
    padding: 0.4rem 0.6rem;
    border: 1px solid #d6d3d1;
    border-radius: 0.375rem;
    font-size: 0.95rem;
  }

  button {
    padding: 0.4rem 0.75rem;
    border: 1px solid #292524;
    background: #292524;
    color: #fff;
    border-radius: 0.375rem;
    cursor: pointer;
    font-size: 0.85rem;
  }

  button.ghost {
    background: transparent;
    color: #292524;
  }

  button:disabled {
    opacity: 0.5;
    cursor: default;
  }

  .todos {
    list-style: none;
    margin: 0 0 1rem;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .todos li {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.4rem 0.6rem;
    border: 1px solid #e7e5e4;
    border-radius: 0.375rem;
  }

  .todos li.done .text {
    text-decoration: line-through;
    color: #a8a29e;
  }

  .todos li.empty {
    color: #a8a29e;
    justify-content: center;
  }

  .text {
    flex: 1;
  }

  .deadletters {
    margin-top: 1.5rem;
    border-top: 1px dashed #d6d3d1;
    padding-top: 1rem;
  }

  .deadletters h2 {
    font-size: 1rem;
    margin: 0 0 0.25rem;
  }

  .deadletters ul {
    list-style: none;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    font-size: 0.85rem;
  }

  .deadletters li {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    flex-wrap: wrap;
  }

  .error {
    color: #b91c1c;
  }
</style>
