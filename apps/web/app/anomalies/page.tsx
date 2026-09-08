"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th, severityTone } from "@/components/ui";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";
import { useState } from "react";

export default function AnomaliesPage() {
  const [severity, setSeverity] = useState("");
  const qs = new URLSearchParams({ since_minutes: "1440", limit: "100" });
  if (severity) qs.set("severity", severity);
  const { data, loading } = useApi<{ items: any[]; meta: { total: number } }>(
    `/api/v1/anomalies?${qs.toString()}`,
    { intervalMs: 15000, deps: [severity] },
  );

  return (
    <div>
      <PageHeader
        title="Anomalies"
        desc="Detected by rolling z-score / EWMA / isolation-forest on windowed metrics, with dynamic historical baselines where available."
        right={
          <select
            className="rounded border border-border bg-bg px-2 py-1 text-xs"
            value={severity}
            onChange={(e) => setSeverity(e.target.value)}
          >
            <option value="">all severities</option>
            {["LOW", "MEDIUM", "HIGH", "CRITICAL"].map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        }
      />
      {loading && !data ? (
        <Spinner />
      ) : !data || data.items.length === 0 ? (
        <EmptyState>No anomalies in the last 24h. Trigger the payment-failure scenario to see one.</EmptyState>
      ) : (
        <Card title={`${data.meta.total} anomalies (24h)`}>
          <Table head={<><Th>Detected</Th><Th>Metric</Th><Th>Severity</Th><Th>Observed</Th><Th>Expected</Th><Th>Deviation</Th><Th>Algorithm</Th><Th>Conf.</Th><Th>Status</Th></>}>
            {data.items.map((a) => (
              <Row key={a.id}>
                <Td className="whitespace-nowrap text-muted">{ago(a.detected_at)}</Td>
                <Td><Link href={`/anomalies/${a.id}`} className="text-accent hover:underline">{a.metric}</Link></Td>
                <Td><Badge tone={severityTone(a.severity)}>{a.severity}</Badge></Td>
                <Td>{num(a.observed_value, 3)}</Td>
                <Td className="text-muted">{num(a.expected_value, 3)}</Td>
                <Td>{num(a.deviation, 3)}</Td>
                <Td className="text-xs text-muted">{a.algorithm}</Td>
                <Td>{num(a.confidence, 2)}</Td>
                <Td><Badge>{a.status}</Badge></Td>
              </Row>
            ))}
          </Table>
        </Card>
      )}
    </div>
  );
}
