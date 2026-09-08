"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { api, getRole } from "@/lib/api";
import { ago } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function ProducersPage() {
  const { data, loading, refetch } = useApi<any[]>("/api/v1/producers", { intervalMs: 15000 });
  const [name, setName] = useState("");
  const [types, setTypes] = useState("");
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const isAdmin = getRole() === "ADMIN";

  async function create() {
    setMsg(null);
    try {
      const r = await api<any>("/api/v1/producers", {
        method: "POST",
        body: JSON.stringify({
          name,
          allowed_event_types: types ? types.split(",").map((s) => s.trim()).filter(Boolean) : [],
          rate_limit_per_min: 60000,
        }),
      });
      setCreatedKey(r.api_key);
      setName("");
      setTypes("");
      refetch();
    } catch (e: any) {
      setMsg(e?.message ?? "failed");
    }
  }

  return (
    <div>
      <PageHeader title="Producers" desc="Event sources. API keys are hashed (HMAC-SHA256) — the plaintext is shown once on creation." />
      {isAdmin && (
        <Card title="Create producer">
          <div className="flex flex-wrap items-end gap-2 text-sm">
            <label className="text-xs text-muted">name<input className="mt-1 block w-56 rounded border border-border bg-bg px-2 py-1" value={name} onChange={(e) => setName(e.target.value.trim())} /></label>
            <label className="text-xs text-muted">allowed types (comma, blank=any)<input className="mt-1 block w-72 rounded border border-border bg-bg px-2 py-1" value={types} onChange={(e) => setTypes(e.target.value)} /></label>
            <button className="rounded bg-accent px-3 py-1.5 font-medium text-white disabled:opacity-50" disabled={!name} onClick={create}>Create</button>
            {msg && <span className="text-xs text-danger">{msg}</span>}
          </div>
          {createdKey && (
            <div className="mt-2 rounded border border-warn/40 bg-warn/10 p-2 text-xs">
              API key (shown once): <code className="break-all font-mono">{createdKey}</code>
            </div>
          )}
        </Card>
      )}

      <Card title="Registered producers" className="mt-3">
        {loading && !data ? <Spinner /> : !data || data.length === 0 ? (
          <EmptyState>No producers.</EmptyState>
        ) : (
          <Table head={<><Th>Name</Th><Th>Status</Th><Th>Key prefix</Th><Th>Allowed types</Th><Th>Rate limit/min</Th><Th>Region</Th><Th>Created</Th></>}>
            {data.map((p) => (
              <Row key={p.id}>
                <Td>{p.name}</Td>
                <Td><Badge tone={p.status === "ACTIVE" ? "ok" : "default"}>{p.status}</Badge></Td>
                <Td className="font-mono text-xs">{p.api_key_prefix}…</Td>
                <Td className="text-xs">{p.allowed_event_types.length ? p.allowed_event_types.join(", ") : "any"}</Td>
                <Td>{p.rate_limit_per_min.toLocaleString()}</Td>
                <Td className="text-muted">{p.default_region ?? "—"}</Td>
                <Td className="text-muted">{ago(p.created_at)}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
