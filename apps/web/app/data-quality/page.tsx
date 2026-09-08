"use client";

import { TimeSeries } from "@/components/charts";
import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, StatCard, Table, Td, Th } from "@/components/ui";
import { num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function DataQualityPage() {
  const { data: ov, loading } = useApi<any>("/api/v1/data-quality/overview?minutes=120", {
    intervalMs: 15000,
  });
  const [producer, setProducer] = useState<string | null>(null);
  const ts = useApi<{ points: any[] }>(
    `/api/v1/data-quality/timeseries?minutes=180${producer ? `&producer_name=${encodeURIComponent(producer)}` : ""}`,
    { intervalMs: 15000, deps: [producer] },
  );

  const score = ov?.platform_avg_score ?? 1;
  return (
    <div>
      <PageHeader title="Data Quality" desc="Per-producer Data Quality Score from validity, completeness, uniqueness, timeliness & schema compliance." />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard
          label="Platform DQ score"
          value={num(score, 3)}
          tone={score > 0.95 ? "ok" : score > 0.8 ? "warn" : "danger"}
        />
        <StatCard label="Producers scored" value={num(ov?.producers?.length ?? 0)} />
      </div>

      <Card title="Score over time" className="mt-3" right={
        <select className="rounded border border-border bg-bg px-2 py-1 text-xs" value={producer ?? ""} onChange={(e) => setProducer(e.target.value || null)}>
          <option value="">all producers</option>
          {(ov?.producers ?? []).map((p: any) => <option key={p.producer} value={p.producer}>{p.producer}</option>)}
        </select>
      }>
        {!ts.data ? <Spinner /> : ts.data.points.length === 0 ? (
          <EmptyState>No DQ records yet.</EmptyState>
        ) : (
          <TimeSeries
            data={ts.data.points}
            series={[
              { key: "overall_score", label: "overall" },
              { key: "validity_score", label: "validity" },
              { key: "timeliness_score", label: "timeliness" },
              { key: "schema_compliance_score", label: "schema" },
            ]}
            yFormat={(v) => v.toFixed(2)}
          />
        )}
      </Card>

      <Card title="Per-producer breakdown (last 2h)" className="mt-3">
        {loading && !ov ? <Spinner /> : !ov || ov.producers.length === 0 ? (
          <EmptyState>No data-quality records yet.</EmptyState>
        ) : (
          <Table head={<><Th>Producer</Th><Th>Avg</Th><Th>Min</Th><Th>Events</Th><Th>Invalid schema</Th><Th>Malformed</Th><Th>Missing</Th><Th>Dupe</Th><Th>Future ts</Th><Th>Late</Th></>}>
            {ov.producers.map((p: any) => (
              <Row key={p.producer}>
                <Td>{p.producer}</Td>
                <Td><Badge tone={p.avg_overall_score > 0.95 ? "ok" : p.avg_overall_score > 0.8 ? "warn" : "danger"}>{num(p.avg_overall_score, 3)}</Badge></Td>
                <Td>{num(p.min_overall_score, 3)}</Td>
                <Td>{num(p.total_events)}</Td>
                <Td className={p.invalid_schema ? "text-danger" : ""}>{num(p.invalid_schema)}</Td>
                <Td className={p.malformed_payload ? "text-danger" : ""}>{num(p.malformed_payload)}</Td>
                <Td>{num(p.missing_fields)}</Td>
                <Td>{num(p.duplicate_events)}</Td>
                <Td>{num(p.future_timestamp)}</Td>
                <Td>{num(p.late_events)}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
