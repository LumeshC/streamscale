import { useState } from "react";
import { api } from "../api/endpoints";
import { AsyncBody, Card } from "../components/Card";
import { DataTable } from "../components/DataTable";
import { StatTile } from "../components/StatTile";
import { ColumnChart, bucketize } from "../components/charts/ColumnChart";
import { ViewToggle, type View } from "../components/ViewToggle";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { fillGaps } from "../lib/series";
import { formatCount, formatPercent, formatTime } from "../lib/format";

export function Payments() {
  const { range, window } = useDashboard();
  const [view, setView] = useState<View>("chart");
  const state = usePolling((s) => api.payments(window(), s), range.id);

  return (
    <Card
      id="payments"
      title="Payment success / failure"
      subtitle={range.label}
      actions={<ViewToggle view={view} onChange={setView} label="Payments" />}
    >
      <AsyncBody state={state} isEmpty={(d) => d.summary.total === 0} empty={{ title: "No payments in this range" }}>
        {({ summary, points }) => {
          const rows = points.map((p) => ({ t: p.t, success: p.success, failure: p.failure }));
          const { rows: bars, minutesPerBar } = bucketize(fillGaps(rows, ["success", "failure"]), 60, ["success", "failure"]);
          const label = (t: number) =>
            minutesPerBar === 1
              ? formatTime(new Date(t).toISOString())
              : `${formatTime(new Date(t).toISOString())} – ${formatTime(new Date(t + minutesPerBar * 60_000).toISOString())}`;
          return (
            <>
              <div className="tiles" style={{ marginBottom: 12 }}>
                <StatTile label="Success rate" value={formatPercent(summary.success_rate)} sub={`${formatCount(summary.success)} succeeded`} />
                <StatTile label="Failure rate" value={formatPercent(summary.failure_rate)} sub={`${formatCount(summary.failure)} failed`} />
                <StatTile label="Payments" value={formatCount(summary.total)} />
              </div>
              {view === "chart" ? (
                <ColumnChart
                  ariaLabel="Successful and failed payments over time"
                  height={200}
                  stacked
                  data={bars}
                  series={[
                    { key: "success", label: "Succeeded", color: "var(--series-1)", format: formatCount },
                    { key: "failure", label: "Failed", color: "var(--series-2)", format: formatCount },
                  ]}
                  yTick={formatCount}
                  bucketLabel={label}
                />
              ) : (
                <DataTable
                  caption="Payments per minute"
                  rows={[...points].reverse()}
                  rowKey={(p) => p.t}
                  columns={[
                    { key: "w", header: "Minute", render: (p) => formatTime(p.window_start) },
                    { key: "s", header: "Succeeded", numeric: true, render: (p) => formatCount(p.success) },
                    { key: "f", header: "Failed", numeric: true, render: (p) => formatCount(p.failure) },
                    { key: "r", header: "Failure rate", numeric: true, render: (p) => formatPercent(p.failure_rate) },
                  ]}
                />
              )}
              {minutesPerBar > 1 ? <p className="footnote">Each column sums {minutesPerBar} minutes.</p> : null}
            </>
          );
        }}
      </AsyncBody>
    </Card>
  );
}
