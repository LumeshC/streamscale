import { useState } from "react";
import { api } from "../api/endpoints";
import type { Latency as L } from "../api/types";
import { AsyncBody, Card } from "../components/Card";
import { DataTable } from "../components/DataTable";
import { StatTile } from "../components/StatTile";
import { TimeSeriesChart } from "../components/charts/TimeSeriesChart";
import { ViewToggle, type View } from "../components/ViewToggle";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatCount, formatMs, formatTime } from "../lib/format";

const CAP_MIN = 180; // API caps latency ranges at 3 h (percentiles read raw rows)

export function Latency() {
  const { range, window } = useDashboard();
  const [view, setView] = useState<View>("chart");
  const state = usePolling((s) => api.latency(window(CAP_MIN), s), range.id);
  const capped = range.minutes > CAP_MIN;

  return (
    <Card
      id="latency"
      title="Processing latency"
      subtitle={`End-to-end: event created → persisted · ${capped ? "last 3 hours (max)" : range.label}`}
      actions={<ViewToggle view={view} onChange={setView} label="Latency" />}
    >
      <AsyncBody<L> state={state} isEmpty={(d) => d.summary.count === 0} empty={{ title: "No events ingested in this range" }}>
        {(d) => (
          <>
            <div className="tiles" style={{ marginBottom: 12 }}>
              <StatTile label="p50" value={formatMs(d.summary.p50_ms)} />
              <StatTile label="p95" value={formatMs(d.summary.p95_ms)} />
              <StatTile label="p99" value={formatMs(d.summary.p99_ms)} sub={`max ${formatMs(d.summary.max_ms)}`} />
            </div>
            {view === "chart" ? (
              <TimeSeriesChart
                ariaLabel="Latency percentiles per minute"
                height={200}
                data={d.data.map((p) => ({ t: Date.parse(p.minute), p50: p.p50_ms, p95: p.p95_ms, p99: p.p99_ms }))}
                series={[
                  { key: "p50", label: "p50", color: "var(--ord-1)", format: formatMs },
                  { key: "p95", label: "p95", color: "var(--ord-2)", format: formatMs },
                  { key: "p99", label: "p99", color: "var(--ord-3)", format: formatMs },
                ]}
                yTick={(v) => `${Math.round(v)} ms`}
              />
            ) : (
              <DataTable
                caption="Latency percentiles per minute"
                rows={[...d.data].reverse()}
                rowKey={(p) => p.minute}
                columns={[
                  { key: "m", header: "Minute", render: (p) => formatTime(p.minute) },
                  { key: "n", header: "Events", numeric: true, render: (p) => formatCount(p.count) },
                  { key: "p50", header: "p50", numeric: true, render: (p) => formatMs(p.p50_ms) },
                  { key: "p95", header: "p95", numeric: true, render: (p) => formatMs(p.p95_ms) },
                  { key: "p99", header: "p99", numeric: true, render: (p) => formatMs(p.p99_ms) },
                ]}
              />
            )}
            <p className="footnote">Measured over {formatCount(d.summary.count)} events (ingested_at − event timestamp).</p>
          </>
        )}
      </AsyncBody>
    </Card>
  );
}
