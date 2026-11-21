import { api } from "../api/endpoints";
import type { Breakdown } from "../api/types";
import { AsyncBody, Card } from "../components/Card";
import { BarList } from "../components/charts/BarList";
import { useDashboard } from "../hooks/useDashboard";
import { usePolling } from "../hooks/usePolling";
import { formatCount, humanize } from "../lib/format";

/** Shared section for the region and event-type breakdowns. */
function DistributionCard({ id, title, kind }: { id: string; title: string; kind: "regions" | "eventTypes" }) {
  const { range, window } = useDashboard();
  const state = usePolling((s) => (kind === "regions" ? api.regions(window(), s) : api.eventTypes(window(), s)), range.id);
  return (
    <Card id={id} title={title} subtitle={state.data ? `${formatCount(state.data.total_events)} events · ${range.label}` : range.label}>
      <AsyncBody<Breakdown> state={state} isEmpty={(d) => d.total_events === 0} empty={{ title: "No events in this range" }} skeletonHeight={180}>
        {(d) => (
          <BarList
            ariaLabel={title}
            items={d.data.map((i) => ({
              key: i.key,
              label: kind === "eventTypes" ? humanize(i.key) : i.key,
              value: i.events,
              share: i.share,
            }))}
          />
        )}
      </AsyncBody>
    </Card>
  );
}

export const Regions = () => <DistributionCard id="regions" title="Regional distribution" kind="regions" />;
export const EventTypes = () => <DistributionCard id="event-types" title="Event type distribution" kind="eventTypes" />;
