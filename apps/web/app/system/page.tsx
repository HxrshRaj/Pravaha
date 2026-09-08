"use client";

import { PageHeader } from "@/components/shell";
import { Card, Dot, EmptyState, Spinner } from "@/components/ui";
import { ago } from "@/lib/format";
import { useApi, useSSEValue } from "@/lib/hooks";

export default function SystemHealthPage() {
  const live = useSSEValue<any>("/api/v1/live/system-health", "system-health");
  const poll = useApi<any>("/api/v1/system/health", { intervalMs: 8000 });
  const h = live.value ?? poll.data;
  const topics = useApi<any>("/api/v1/system/topics");

  if (!h) return <Spinner label="Checking dependencies…" />;

  return (
    <div>
      <PageHeader
        title="System Health"
        desc="Live dependency + processor checks. Postgres and Kafka are required; Redis and AI degrade gracefully."
      />
      <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        {Object.entries(h.checks ?? {}).map(([name, c]: [string, any]) => (
          <Card key={name} title={<span className="flex items-center gap-2"><Dot ok={!!c.up} />{name}</span>}>
            <div className="space-y-1 text-sm">
              <div className="flex justify-between"><span className="text-muted">up</span><span>{String(c.up)}</span></div>
              {c.latency_ms != null && <div className="flex justify-between"><span className="text-muted">latency</span><span>{c.latency_ms.toFixed(1)} ms</span></div>}
              {c.providers && <div className="flex justify-between"><span className="text-muted">providers</span><span>{c.providers.join(", ") || "none"}</span></div>}
              {c.required === false && <div className="text-xs text-muted">not required for readiness</div>}
              {c.last_write && <div className="flex justify-between"><span className="text-muted">last write</span><span>{ago(c.last_write)}</span></div>}
              {c.last_sample && <div className="flex justify-between"><span className="text-muted">last sample</span><span>{ago(c.last_sample)}</span></div>}
              {c.error && <div className="text-xs text-danger">{c.error}</div>}
            </div>
          </Card>
        ))}
      </div>

      <Card title="Kafka topics" className="mt-3">
        {!topics.data ? <Spinner /> : (
          <ul className="space-y-1 text-sm">
            {topics.data.topics.map((t: any) => (
              <li key={t.name} className="border-b border-border/50 py-1">
                <span className="font-mono text-accent">{t.name}</span>
                <span className="ml-2 text-xs text-muted">{t.partitions}p · retention {Math.round(t.retention_ms / 86400000)}d</span>
                <div className="text-xs text-muted">{t.purpose}</div>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}
