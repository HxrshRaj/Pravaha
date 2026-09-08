"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th, severityTone } from "@/components/ui";
import { api } from "@/lib/api";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

const emptyRule = {
  name: "",
  metric: "payment_failure_rate",
  operator: "gt",
  threshold: 0.1,
  duration_seconds: 120,
  severity: "HIGH",
  cooldown_seconds: 600,
};

export default function AlertsPage() {
  const rules = useApi<any[]>("/api/v1/alerts/rules", { intervalMs: 15000 });
  const fired = useApi<any[]>("/api/v1/alerts?since_minutes=1440&limit=100", { intervalMs: 8000 });
  const [form, setForm] = useState<any>(emptyRule);
  const [msg, setMsg] = useState<string | null>(null);

  async function create() {
    setMsg(null);
    try {
      await api("/api/v1/alerts/rules", { method: "POST", body: JSON.stringify(form) });
      setForm(emptyRule);
      rules.refetch();
      setMsg("rule created");
    } catch (e: any) {
      setMsg(e?.message ?? "failed");
    }
  }
  async function toggle(r: any) {
    await api(`/api/v1/alerts/rules/${r.id}`, { method: "PATCH", body: JSON.stringify({ enabled: !r.enabled }) });
    rules.refetch();
  }

  return (
    <div>
      <PageHeader title="Alerts" desc="Threshold & trend rules evaluated against windowed metrics, with duration + cooldown to prevent alert storms." />
      <div className="grid gap-3 lg:grid-cols-[1fr_360px]">
        <Card title="Fired alerts (24h)">
          {fired.loading && !fired.data ? <Spinner /> : !fired.data || fired.data.length === 0 ? (
            <EmptyState>No alerts fired.</EmptyState>
          ) : (
            <Table head={<><Th>When</Th><Th>Rule</Th><Th>Metric</Th><Th>Severity</Th><Th>Observed</Th><Th>Threshold</Th></>}>
              {fired.data.map((a) => (
                <Row key={a.id}>
                  <Td className="whitespace-nowrap text-muted">{ago(a.fired_at)}</Td>
                  <Td>{a.rule_name}</Td>
                  <Td>{a.metric}</Td>
                  <Td><Badge tone={severityTone(a.severity)}>{a.severity}</Badge></Td>
                  <Td>{num(a.observed_value, 3)}</Td>
                  <Td className="text-muted">{num(a.threshold, 3)}</Td>
                </Row>
              ))}
            </Table>
          )}
        </Card>

        <Card title="New rule">
          <div className="space-y-2 text-sm">
            <input className="w-full rounded border border-border bg-bg px-2 py-1" placeholder="name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
            <input className="w-full rounded border border-border bg-bg px-2 py-1" placeholder="metric" value={form.metric} onChange={(e) => setForm({ ...form, metric: e.target.value })} />
            <div className="flex gap-2">
              <select className="rounded border border-border bg-bg px-2 py-1" value={form.operator} onChange={(e) => setForm({ ...form, operator: e.target.value })}>
                {["gt", "gte", "lt", "lte", "drop_pct", "rise_pct"].map((o) => <option key={o}>{o}</option>)}
              </select>
              <input type="number" step="any" className="w-24 rounded border border-border bg-bg px-2 py-1" value={form.threshold} onChange={(e) => setForm({ ...form, threshold: Number(e.target.value) })} />
            </div>
            <div className="flex gap-2">
              <label className="text-xs text-muted">dur(s)<input type="number" className="mt-0.5 w-full rounded border border-border bg-bg px-2 py-1" value={form.duration_seconds} onChange={(e) => setForm({ ...form, duration_seconds: Number(e.target.value) })} /></label>
              <label className="text-xs text-muted">cooldown(s)<input type="number" className="mt-0.5 w-full rounded border border-border bg-bg px-2 py-1" value={form.cooldown_seconds} onChange={(e) => setForm({ ...form, cooldown_seconds: Number(e.target.value) })} /></label>
            </div>
            <select className="w-full rounded border border-border bg-bg px-2 py-1" value={form.severity} onChange={(e) => setForm({ ...form, severity: e.target.value })}>
              {["LOW", "MEDIUM", "HIGH", "CRITICAL"].map((s) => <option key={s}>{s}</option>)}
            </select>
            <button className="w-full rounded bg-accent px-3 py-1.5 font-medium text-white" onClick={create}>Create rule</button>
            {msg && <div className="text-xs text-accent">{msg}</div>}
          </div>
        </Card>
      </div>

      <Card title="Rules" className="mt-3">
        {rules.loading && !rules.data ? <Spinner /> : !rules.data || rules.data.length === 0 ? (
          <EmptyState>No rules yet.</EmptyState>
        ) : (
          <Table head={<><Th>Name</Th><Th>Condition</Th><Th>Duration</Th><Th>Severity</Th><Th>Cooldown</Th><Th>Enabled</Th><Th>Last fired</Th></>}>
            {rules.data.map((r) => (
              <Row key={r.id}>
                <Td>{r.name}</Td>
                <Td className="font-mono text-xs">{r.metric} {r.operator} {r.threshold}</Td>
                <Td>{r.duration_seconds}s</Td>
                <Td><Badge tone={severityTone(r.severity)}>{r.severity}</Badge></Td>
                <Td>{r.cooldown_seconds}s</Td>
                <Td><button onClick={() => toggle(r)} className="text-xs text-accent hover:underline">{r.enabled ? "enabled" : "disabled"}</button></Td>
                <Td className="text-xs text-muted">{r.last_fired_at ? ago(r.last_fired_at) : "—"}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
