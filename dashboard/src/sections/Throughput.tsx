import { useState } from "react";
import { api } from "../api/endpoints";
import { AsyncBody, Card } from "../components/Card";
import { DataTable } from "../components/DataTable";
import { TimeSeriesChart } from "../components/charts/TimeSeriesChart";
import { ViewToggle, type View } from "../components/ViewToggle";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatCount, formatRate, formatTime } from "../lib/format";

type Res = "1m" | "1s";

export function Throughput() {
  const { range, window } = useDashboard();
  const [res, setRes] = useState<Res>("1m");
  const [view, setView] = useState<View>("chart");
  // Per-second resolution is capped to 15 minutes (900 points).
  const state = usePolling((s) => api.events(res === "1s" ? window(15) : window(), res, s), `${range.id}:${res}`);

  const perSecond = res === "1s";
  return (
    <Card
      id="throughput"
      title="Event throughput"
      subtitle={perSecond ? "Events per second · last 15 min (1s windows)" : `Events per minute · ${range.label}`}
      actions={
        <>
          <div className="segmented" role="group" aria-label="Resolution">
            <button aria-pressed={res === "1m"} onClick={() => setRes("1m")}>Per min</button>
            <button aria-pressed={res === "1s"} onClick={() => setRes("1s")}>Per sec</button>
          </div>
          <ViewToggle view={view} onChange={setView} label="Throughput" />
        </>
      }
    >
      <AsyncBody state={state} isEmpty={(d) => d.points.length === 0} empty={{ title: "No closed windows in this range", hint: "Is the event generator running?" }}>
        {({ points }) => {
          const key = perSecond ? "events_per_second" : "events";
          const vals = points.map((p) => (perSecond ? p.events_per_second : p.events));
          const peak = Math.max(...vals);
          const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
          const unit = perSecond ? "/s" : "/min";
          return (
            <>
              <p className="footnote" style={{ marginTop: 0, marginBottom: 8 }}>
                Avg <strong>{formatRate(avg)}{unit}</strong> · peak <strong>{formatRate(peak)}{unit}</strong>
              </p>
              {view === "chart" ? (
                <TimeSeriesChart
                  ariaLabel={`Events ${perSecond ? "per second" : "per minute"} over time`}
                  data={points.map((p) => ({ t: p.t, v: perSecond ? p.events_per_second : p.events }))}
                  series={[{ key: "v", label: perSecond ? "Events / s" : "Events / min", color: "var(--series-1)", area: true, format: (v) => `${formatRate(v)}${unit}` }]}
                  yTick={formatCount}
                  seconds={perSecond}
                />
              ) : (
                <DataTable
                  caption="Event throughput by window"
                  rows={[...points].reverse()}
                  rowKey={(p) => p.t}
                  columns={[
                    { key: "w", header: "Window start", render: (p) => formatTime(p.window_start, perSecond) },
                    { key: key, header: perSecond ? "Events / s" : "Events", numeric: true, render: (p) => formatRate(perSecond ? p.events_per_second : p.events) },
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
