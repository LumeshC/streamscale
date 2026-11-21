import { api } from "../api/endpoints";
import type { Check, Health, Overview } from "../api/types";
import { AsyncBody, Card } from "../components/Card";
import { StatTile } from "../components/StatTile";
import { StatusBadge, type Tone } from "../components/StatusBadge";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatAmount, formatCount, formatPercent, formatRate, formatTime } from "../lib/format";

function depTone(c?: Check): Tone {
  if (!c) return "muted";
  return c.status === "ok" ? "good" : c.status === "disabled" ? "muted" : "critical";
}

function overallTone(h: Health): Tone {
  return h.status === "ok" ? "good" : h.status === "degraded" ? "warning" : "critical";
}

export function SystemOverview() {
  const { range, window } = useDashboard();
  const health = usePolling((s) => api.health(s), "health");
  const overview = usePolling((s) => api.overview(window(), s), range.id);

  const h = health.data;
  return (
    <Card
      id="overview"
      title="System overview"
      subtitle={`${range.label} · totals from closed 1-minute windows; live figures from the last 60 seconds`}
    >
      <div className="deps" aria-label="Dependency status">
        {health.error ? (
          // A failed health check outranks stale badges: say so plainly.
          <StatusBadge tone="critical" label="Analytics API unreachable" detail={health.error.message} />
        ) : h ? (
          <>
            <StatusBadge tone={overallTone(h)} label={`System ${h.status}`} />
            {(["postgres", "redis", "kafka"] as const).map((k) => {
              const c = h.checks[k];
              return (
                <StatusBadge
                  key={k}
                  tone={depTone(c)}
                  label={`${k === "postgres" ? "PostgreSQL" : k === "redis" ? "Redis" : "Kafka"} ${c?.status ?? "unknown"}`}
                  detail={c?.error ?? (c?.latency_ms != null ? `${c.latency_ms} ms` : undefined)}
                />
              );
            })}
          </>
        ) : (
          <StatusBadge tone="muted" label="Checking dependencies…" />
        )}
      </div>
      <AsyncBody<Overview> state={overview} skeletonHeight={96}>
        {(o) => (
          <>
            <div className="tiles">
              <StatTile
                label="Live throughput"
                value={o.live ? `${formatRate(o.live.events_per_second)}/s` : "—"}
                sub={o.live ? `${formatCount(o.live.events)} events in last 60s` : "live view unavailable"}
                hero
              />
              <StatTile label="Events" value={formatCount(o.totals.events)} sub={`${formatRate(o.totals.events_per_minute)} / min avg`} />
              <StatTile label="Revenue" value={formatAmount(o.totals.revenue)} sub={`${formatCount(o.totals.purchases)} purchases`} />
              <StatTile
                label="Payment success"
                value={formatPercent(o.totals.payment_success_rate)}
                sub={`${formatCount(o.totals.payment_failure)} failed`}
              />
              <StatTile
                label="Active users now"
                value={o.live ? formatCount(o.live.active_users) : "—"}
                sub="seen in last 5 min"
              />
            </div>
            <p className="footnote">
              {o.coverage.last_window_end
                ? `Totals cover ${formatCount(o.totals.windows)} closed windows through ${formatTime(o.coverage.last_window_end)}.`
                : "No closed windows in this range yet."}
              {o.degraded.includes("live") ? " Live view degraded: Redis is unavailable." : ""}
            </p>
          </>
        )}
      </AsyncBody>
    </Card>
  );
}
