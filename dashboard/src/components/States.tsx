import { ApiError } from "../api/client";
import { AlertTriangle, Refresh } from "./Icons";

export function Skeleton({ height = 200, lines }: { height?: number; lines?: number }) {
  if (lines) {
    return (
      <div aria-hidden style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {Array.from({ length: lines }, (_, i) => (
          <div key={i} className="skeleton" style={{ height: 14, width: `${90 - i * 12}%` }} />
        ))}
      </div>
    );
  }
  return <div className="skeleton" aria-hidden style={{ height }} />;
}

export function LoadingState({ height = 200, label = "Loading" }: { height?: number; label?: string }) {
  return (
    <div role="status" aria-live="polite">
      <span className="sr-only">{label}…</span>
      <Skeleton height={height} />
    </div>
  );
}

export function ErrorState({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  return (
    <div className="state" role="alert">
      <AlertTriangle size={18} />
      <strong>{error.userMessage}</strong>
      <small>
        {error.message}
        {error.status && !error.message.startsWith("HTTP") ? ` (HTTP ${error.status})` : ""}
      </small>
      {error.requestId ? <code>request {error.requestId}</code> : null}
      {onRetry ? (
        <button className="btn" onClick={onRetry} style={{ marginTop: 6 }}>
          <Refresh /> Retry
        </button>
      ) : null}
    </div>
  );
}

export function EmptyState({ title, hint }: { title: string; hint?: string }) {
  return (
    <div className="state">
      <strong>{title}</strong>
      {hint ? <small>{hint}</small> : null}
    </div>
  );
}
