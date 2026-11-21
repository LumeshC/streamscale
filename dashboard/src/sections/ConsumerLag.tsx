import { useEffect, useRef, useState } from "react";
import { api } from "../api/endpoints";
import type { ConsumerLag as Lag } from "../api/types";
import { AsyncBody, Card } from "../components/Card";
import { StatTile } from "../components/StatTile";
import { StatusBadge, type Tone } from "../components/StatusBadge";
import { BarList } from "../components/charts/BarList";
import { Sparkline } from "../components/charts/Sparkline";
import { usePolling } from "../hooks/usePolling";
import { formatCount } from "../lib/format";

/** Lag thresholds (messages). At ~100 events/s, 1k ≈ 10 s behind. */
export function lagTone(total: number): { tone: Tone; label: string } {
  if (total <= 1_000) return { tone: "good", label: "Keeping up" };
  if (total <= 10_000) return { tone: "warning", label: "Elevated lag" };
  return { tone: "critical", label: "Falling behind" };
}

const HISTORY = 60;

export function ConsumerLag() {
  // Lag is point-in-time; it does not depend on the selected range.
  const state = usePolling((s) => api.consumerLag(s), "lag");
  // Session-local history of real samples (not persisted, not synthesized).
  const history = useRef<{ at: string; lag: number }[]>([]);
  const [, bump] = useState(0);
  useEffect(() => {
    const d = state.data;
    if (!d) return;
    const h = history.current;
    if (h.length && h[h.length - 1].at === d.as_of) return;
    h.push({ at: d.as_of, lag: d.total_lag });
    if (h.length > HISTORY) h.shift();
    bump((x) => x + 1);
  }, [state.data]);

  return (
    <Card id="lag" title="Kafka consumer lag" subtitle={state.data ? `group ${state.data.group} · topic ${state.data.topic}` : "Processor consumer group"}>
      <AsyncBody<Lag> state={state} skeletonHeight={220}>
        {(d) => {
          const t = lagTone(d.total_lag);
          const total = Math.max(1, d.total_lag);
          return (
            <>
              <div className="deps">
                <StatusBadge tone={t.tone} label={t.label} />
              </div>
              <div className="tiles" style={{ gridTemplateColumns: "1fr" }}>
                <StatTile label="Messages behind" value={formatCount(d.total_lag)} sub={`of ${formatCount(d.end_offset_total)} written`} hero />
              </div>
              {history.current.length > 1 ? (
                <div style={{ margin: "10px 0 4px" }}>
                  <Sparkline values={history.current.map((h) => h.lag)} ariaLabel="Total lag, samples this session" />
                  <p className="footnote" style={{ margin: 0 }}>Last {history.current.length} samples this session</p>
                </div>
              ) : null}
              <h3 className="sr-only">Lag by partition</h3>
              <div style={{ marginTop: 12 }}>
                <BarList
                  ariaLabel="Lag by partition"
                  items={d.partitions.map((p) => ({ key: String(p.partition), label: `Partition ${p.partition}`, value: p.lag, share: p.lag / total }))}
                />
              </div>
            </>
          );
        }}
      </AsyncBody>
    </Card>
  );
}
