"use client";

import { login } from "@/lib/api";
import { useRouter } from "next/navigation";
import { useState } from "react";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("admin@pravaha.local");
  const [password, setPassword] = useState("admin12345");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setErr(null);
    try {
      await login(email, password);
      router.replace("/");
    } catch (e: any) {
      setErr(e?.message ?? "login failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center px-4">
      <form
        onSubmit={onSubmit}
        className="w-full max-w-sm space-y-4 rounded-lg border border-border bg-surface p-6"
      >
        <div>
          <h1 className="text-lg font-bold tracking-tight">Pravāha</h1>
          <p className="text-sm text-muted">Sign in to the control plane</p>
        </div>
        {err && (
          <div className="rounded border border-danger/40 bg-danger/10 p-2 text-sm text-danger">
            {err}
          </div>
        )}
        <label className="block text-sm">
          <span className="text-muted">Email</span>
          <input
            className="mt-1 w-full rounded border border-border bg-bg px-2 py-1.5 text-sm outline-none focus:border-accent"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="username"
          />
        </label>
        <label className="block text-sm">
          <span className="text-muted">Password</span>
          <input
            type="password"
            className="mt-1 w-full rounded border border-border bg-bg px-2 py-1.5 text-sm outline-none focus:border-accent"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
          />
        </label>
        <button
          disabled={busy}
          className="w-full rounded bg-accent px-3 py-2 text-sm font-medium text-white disabled:opacity-60"
        >
          {busy ? "Signing in…" : "Sign in"}
        </button>
        <p className="text-xs text-muted">
          Default dev credentials are pre-filled. Change them via
          <code className="mx-1 rounded bg-surface2 px-1">BOOTSTRAP_ADMIN_*</code>.
        </p>
      </form>
    </div>
  );
}
