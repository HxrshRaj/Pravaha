"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Spinner } from "@/components/ui";
import { API_BASE, getEmail, getRole } from "@/lib/api";
import { useApi } from "@/lib/hooks";

export default function SettingsPage() {
  const me = useApi<any>("/api/v1/auth/me");
  const ai = useApi<any>("/api/v1/ai/status");
  const users = useApi<any[]>("/api/v1/auth/users");

  return (
    <div>
      <PageHeader title="Settings" desc="Session, roles and platform configuration surface." />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card title="Session">
          <dl className="grid grid-cols-[120px_1fr] gap-y-1.5 text-sm">
            <dt className="text-muted">email</dt><dd>{getEmail() ?? me.data?.email ?? "—"}</dd>
            <dt className="text-muted">role</dt><dd><Badge tone="accent">{getRole() ?? me.data?.role ?? "—"}</Badge></dd>
            <dt className="text-muted">API base</dt><dd className="font-mono text-xs">{API_BASE}</dd>
          </dl>
          <p className="mt-3 text-xs text-muted">
            Roles: VIEWER (dashboards) · ANALYST (event inspection, start AI) · ENGINEER (replay, DLQ, pipelines, alert rules) · ADMIN (producers, schemas, users).
          </p>
        </Card>

        <Card title="AI provider">
          {!ai.data ? <Spinner /> : (
            <dl className="grid grid-cols-[140px_1fr] gap-y-1.5 text-sm">
              <dt className="text-muted">provider order</dt><dd>{ai.data.provider_order.join(", ")}</dd>
              <dt className="text-muted">available</dt><dd>{ai.data.available_providers.join(", ") || "none (mock only)"}</dd>
              <dt className="text-muted">core depends on AI</dt><dd>{String(ai.data.core_streaming_dependent_on_ai)}</dd>
            </dl>
          )}
          <p className="mt-3 text-xs text-muted">
            Configure real providers via <code>AI_PROVIDER_ORDER</code>, <code>AI_OPENAI_API_KEY</code>, <code>AI_GROQ_API_KEY</code>.
          </p>
        </Card>
      </div>

      <Card title="Users" className="mt-3">
        {users.error ? (
          <div className="text-sm text-muted">ADMIN role required to list users.</div>
        ) : !users.data ? (
          <Spinner />
        ) : users.data.length === 0 ? (
          <EmptyState>No users.</EmptyState>
        ) : (
          <ul className="space-y-1 text-sm">
            {users.data.map((u) => (
              <li key={u.id} className="flex items-center justify-between border-b border-border/50 py-1">
                <span>{u.email}</span>
                <span className="flex items-center gap-2">
                  <Badge tone="accent">{u.role}</Badge>
                  <span className="text-xs text-muted">{u.is_active ? "active" : "inactive"}</span>
                </span>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}
