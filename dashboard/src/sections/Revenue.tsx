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
import { formatAmount, formatCount, formatTime } from "../lib/format";

export function Revenue() {
  const { range, window } = useDashboard();
  const [view, setView] = useState<View>("chart");
  const state = usePolling((s) => api.revenue(window(), s), range.id);

  return (
    <Card
      id="revenue"
      title="Revenue"
      subtitle={`${range.label} · purchase amounts summed across currencies`}
      actions={<ViewToggle view={view} onChange={setView} label="Revenue" />}
    >
      <AsyncBody state={state} isEmpty={(d) => d.points.length === 0} empty={{ title: "No closed windows in this range" }}>
        {({ summary, points }) => {
          const rows = points.map((p) => ({ t: p.t, revenue: Number(p.revenue), purchases: p.purchases }));
          const { rows: bars, minutesPerBar } = bucketize(fillGaps(rows, ["revenue", "purchases"]), 60, ["revenue", "purchases"]);
          const label = (t: number) =>
            minutesPerBar === 1
              ? formatTime(new Date(t).toISOString())
              : `${formatTime(new Date(t).toISOString())} – ${formatTime(new Date(t + minutesPerBar * 60_000).toISOString())}`;
          return (
            <>
              <div className="tiles" style={{ marginBottom: 12 }}>
                <StatTile label="Total revenue" value={formatAmount(summary.revenue)} />
                <StatTile label="Per minute" value={formatAmount(summary.revenue_per_minute)} />
                <StatTile label="Avg order value" value={formatAmount(summary.average_order_value, false)} sub={`${formatCount(summary.purchases)} purchases`} />
              </div>
              {view === "chart" ? (
                <ColumnChart
                  ariaLabel={`Revenue per ${minutesPerBar === 1 ? "minute" : `${minutesPerBar} minutes`}`}
                  height={200}
                  data={bars}
                  series={[{ key: "revenue", label: "Revenue", color: "var(--series-1)", format: (v) => formatAmount(v, false) }]}
                  yTick={(v) => formatAmount(v)}
                  bucketLabel={label}
                />
              ) : (
                <DataTable
                  caption="Revenue per minute"
                  rows={[...points].reverse()}
                  rowKey={(p) => p.t}
                  columns={[
                    { key: "w", header: "Minute", render: (p) => formatTime(p.window_start) },
                    { key: "p", header: "Purchases", numeric: true, render: (p) => formatCount(p.purchases) },
                    { key: "r", header: "Revenue", numeric: true, render: (p) => formatAmount(p.revenue, false) },
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
