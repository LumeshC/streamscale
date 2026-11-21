import { useState } from "react";
import { api } from "../api/endpoints";
import type { ErrorList } from "../api/types";
import { AsyncBody, Card } from "../components/Card";
import { DataTable } from "../components/DataTable";
import { StatTile } from "../components/StatTile";
import { StatusBadge } from "../components/StatusBadge";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatCount, formatDateTime, formatPercent, humanize } from "../lib/format";

const CLASSES = ["all", "malformed", "permanent", "transient"];

export function RecentErrors() {
  const { range, window } = useDashboard();
  const [cls, setCls] = useState("all");
  const state = usePolling((s) => api.errors(window(), cls, s), `${range.id}:${cls}`);

  return (
    <Card
      id="errors"
      title="Recent errors"
      subtitle={`Messages sent to the dead-letter queue · ${range.label}`}
      actions={
        <label>
          <span className="sr-only">Failure class</span>
          <select className="select" value={cls} onChange={(e) => setCls(e.target.value)}>
            {CLASSES.map((c) => (
              <option key={c} value={c}>
                {c === "all" ? "All classes" : humanize(c)}
              </option>
            ))}
          </select>
        </label>
      }
    >
      <AsyncBody<ErrorList> state={state} skeletonHeight={160}>
        {(d) => (
          <>
            <div className="tiles" style={{ marginBottom: 12 }}>
              <StatTile label="Dead-lettered" value={formatCount(d.totals.dlq)} />
              <StatTile label="Error rate" value={formatPercent(d.totals.error_rate, 2)} sub={`of ${formatCount(d.totals.processed + d.totals.duplicate + d.totals.dlq)} messages`} />
              <StatTile label="Duplicates skipped" value={formatCount(d.totals.duplicate)} />
            </div>
            {d.data.length === 0 ? (
              <div className="deps">
                <StatusBadge tone="good" label="No errors in this range" />
              </div>
            ) : (
              <>
                {d.by_reason.length ? (
                  <p className="footnote" style={{ marginTop: 0 }}>
                    Top reasons:{" "}
                    {d.by_reason.slice(0, 4).map((r, i) => (
                      <span key={`${r.failure_class}/${r.failure_reason}`}>
                        {i ? " · " : ""}
                        {humanize(r.failure_reason)} ({formatCount(r.count)})
                      </span>
                    ))}
                  </p>
                ) : null}
                <DataTable
                  caption="Most recent dead-lettered messages"
                  rows={d.data}
                  rowKey={(e) => e.id}
                  columns={[
                    { key: "t", header: "Time", render: (e) => formatDateTime(e.occurred_at) },
                    { key: "c", header: "Class", render: (e) => <span className="pill">{e.failure_class}</span> },
                    { key: "r", header: "Reason", render: (e) => humanize(e.failure_reason) },
                    { key: "e", header: "Event ID", render: (e) => (e.event_id ? <code>{e.event_id}</code> : <span className="pill">unparsed</span>) },
                    { key: "p", header: "Partition / offset", numeric: true, render: (e) => `${e.partition} / ${e.offset}` },
                    { key: "a", header: "Attempts", numeric: true, render: (e) => e.attempts },
                  ]}
                />
                {d.page.next_cursor ? <p className="footnote">Showing the {d.data.length} most recent.</p> : null}
              </>
            )}
          </>
        )}
      </AsyncBody>
    </Card>
  );
}
