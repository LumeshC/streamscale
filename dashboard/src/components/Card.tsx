import type { ReactNode } from "react";
import type { PollState } from "../hooks/usePolling";
import { EmptyState, ErrorState, LoadingState } from "./States";
import { AlertTriangle } from "./Icons";

interface CardProps {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
  className?: string;
  children: ReactNode;
  id?: string;
}

/** A dashboard section frame: title, optional subtitle and actions, body. */
export function Card({ title, subtitle, actions, className = "", children, id }: CardProps) {
  const headingId = id ? `${id}-title` : undefined;
  return (
    <section className={`card ${className}`} aria-labelledby={headingId} id={id}>
      <header className="card-head">
        <div>
          <h2 id={headingId}>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        {actions ? <div className="card-actions">{actions}</div> : null}
      </header>
      {children}
    </section>
  );
}

interface AsyncBodyProps<T> {
  state: PollState<T>;
  /** Treat loaded data as empty (shows the empty state). */
  isEmpty?: (d: T) => boolean;
  empty?: { title: string; hint?: string };
  skeletonHeight?: number;
  children: (data: T) => ReactNode;
}

/**
 * The loading / error / empty / data state machine every section shares.
 * - first load: skeleton
 * - error with no data: error state + retry
 * - error with stale data: keep data, show a non-blocking note
 * - refreshing: keep data at reduced opacity (no layout jump)
 */
export function AsyncBody<T>({ state, isEmpty, empty, skeletonHeight = 200, children }: AsyncBodyProps<T>) {
  const { data, error, loading, refreshing, retry } = state;
  if (loading) return <LoadingState height={skeletonHeight} />;
  if (data === undefined) return error ? <ErrorState error={error} onRetry={retry} /> : null;
  return (
    <div className={`card-body${refreshing ? " is-refreshing" : ""}`} aria-busy={refreshing}>
      {error ? (
        <div className="stale-note" role="status">
          <AlertTriangle size={13} /> Showing last good data — refresh failed: {error.userMessage}
        </div>
      ) : null}
      {isEmpty?.(data) ? <EmptyState title={empty?.title ?? "No data"} hint={empty?.hint} /> : children(data)}
    </div>
  );
}
