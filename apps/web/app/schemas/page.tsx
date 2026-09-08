"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function SchemasPage() {
  const { data, loading } = useApi<any[]>("/api/v1/schemas", { intervalMs: 20000 });
  const [open, setOpen] = useState<string | null>(null);

  return (
    <div>
      <PageHeader title="Schemas" desc="Lightweight JSON-Schema registry per event type with versioning and BACKWARD compatibility checks." />
      {loading && !data ? <Spinner /> : !data || data.length === 0 ? (
        <EmptyState>No schemas registered. The demo generator registers a few with --bootstrap.</EmptyState>
      ) : (
        <div className="space-y-2">
          {data.map((s) => (
            <Card
              key={s.id}
              title={
                <span className="flex items-center gap-2">
                  {s.event_type}
                  <Badge tone="accent">v{s.active_version ?? "—"} active</Badge>
                  <Badge>{s.compatibility}</Badge>
                  <span className="text-xs font-normal text-muted">{s.versions.length} version(s)</span>
                </span>
              }
              right={<button className="text-xs text-accent hover:underline" onClick={() => setOpen(open === s.id ? null : s.id)}>{open === s.id ? "hide" : "view"}</button>}
            >
              {open === s.id ? (
                <Table head={<><Th>Version</Th><Th>Active</Th><Th>By</Th><Th>Notes</Th><Th>Schema</Th></>}>
                  {s.versions.map((v: any) => (
                    <Row key={v.id}>
                      <Td>v{v.version}</Td>
                      <Td>{v.is_active ? <Badge tone="ok">active</Badge> : ""}</Td>
                      <Td className="text-xs text-muted">{v.created_by ?? "—"}</Td>
                      <Td className="text-xs">{v.notes}</Td>
                      <Td><pre className="max-h-40 max-w-md overflow-auto rounded bg-bg p-2 font-mono text-[11px]">{JSON.stringify(v.json_schema, null, 1)}</pre></Td>
                    </Row>
                  ))}
                </Table>
              ) : (
                <p className="text-sm text-muted">{s.description || "—"}</p>
              )}
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
