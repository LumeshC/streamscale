import { AlertTriangle, CheckCircle, MinusCircle, XCircle } from "./Icons";

export type Tone = "good" | "warning" | "critical" | "muted";

/** Status is never color alone: icon + text label always accompany it. */
export function StatusBadge({ tone, label, detail }: { tone: Tone; label: string; detail?: string }) {
  const Icon = { good: CheckCircle, warning: AlertTriangle, critical: XCircle, muted: MinusCircle }[tone];
  return (
    <span className={`status status-${tone}`} title={detail}>
      <Icon size={13} />
      {label}
    </span>
  );
}
