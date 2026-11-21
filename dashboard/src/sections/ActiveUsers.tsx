import { useState } from "react";
import { api } from "../api/endpoints";
import { AsyncBody, Card } from "../components/Card";
import { DataTable } from "../components/DataTable";
import { StatTile } from "../components/StatTile";
import { TimeSeriesChart } from "../components/charts/TimeSeriesChart";
import { ViewToggle, type View } from "../components/ViewToggle";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatCount, formatTime } from "../lib/format";

export function ActiveUsers() {
  const { range, window } = useDashboard();
  const [view, setView] = useState<View>("chart");
  const series = usePolling((s) => api.events(window(), "1m", s), range.id);
  const overview = usePolling((s) => api.overview(window(), s), `au:${range.id}`);

  const live = overview.data?.live;
  return (
    <Card
      id="active-users"
      title="Active users"
      subtitle={`Distinct users per minute · ${range.label}`}
      actions={<ViewToggle view={view} onChange={setView} label="Active users" />}
    >
      <AsyncBody state={series} isEmpty={(d) => d.points.length === 0} empty={{ title: "No closed windows in this range" }}>
        {({ points }) => {
          const peak = Math.max(...points.map((p) => p.active_users));
          return (
            <>
              <div className="tiles" style={{ marginBottom: 12 }}>
                <StatTile label="Active now" value={live ? formatCount(live.active_users) : "—"} sub={live ? "seen in last 5 min (live)" : "live view unavailable"} />
                <StatTile label="Peak per minute" value={formatCount(peak)} sub="distinct users in one window" />
              </div>
              {view === "chart" ? (
                <TimeSeriesChart
                  ariaLabel="Distinct active users per minute"
                  height={200}
                  data={points.map((p) => ({ t: p.t, u: p.active_users }))}
                  series={[{ key: "u", label: "Active users", color: "var(--series-1)", area: true, format: formatCount }]}
                  yTick={formatCount}
                />
              ) : (
                <DataTable
                  caption="Active users per minute"
                  rows={[...points].reverse()}
                  rowKey={(p) => p.t}
                  columns={[
                    { key: "w", header: "Minute", render: (p) => formatTime(p.window_start) },
                    { key: "u", header: "Active users", numeric: true, render: (p) => formatCount(p.active_users) },
                  ]}
                />
              )}
            </>
          );
        }}
      </AsyncBody>
    </Card>
  );
}
