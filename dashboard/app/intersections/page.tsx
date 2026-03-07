import Link from "next/link";

import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import { getIntersections } from "@/lib/data";

export default async function IntersectionsPage() {
  const intersections = await getIntersections();

  return (
    <DashboardShell
      eyebrow="Intersection Alerts"
      title="Companies appearing in both the current pipeline and RSS opportunities"
    >
      {intersections.length === 0 ? (
        <EmptyState
          title="No intersection alerts yet"
          body="An alert appears when a normalized company match exists between the current sales snapshot and an accepted RSS opportunity event."
        />
      ) : (
        <div className="cards-grid">
          {intersections.map((alert) => (
            <article key={alert.id} className="event-card">
              <div className="row-header">
                <div>
                  <p className="section-kicker">{alert.companyName}</p>
                  <h3>{alert.event.headline}</h3>
                </div>
                <span className="pill pill-alert">Alert {Math.round(alert.alertScore)}</span>
              </div>
              <div className="pill-row">
                <span className="pill">{alert.currentSignal.region}</span>
                <span className="pill">{alert.currentSignal.currentPriority || "No priority"}</span>
                <span className="pill pill-accent">{alert.event.eventType}</span>
                <span className="pill">{alert.event.sourceName}</span>
              </div>
              <p>{alert.alertReason}</p>
              <p className="panel-copy">
                Current action: {alert.currentSignal.currentAction || "Monitor"} · RSS score{" "}
                {Math.round(alert.event.eventScore)}
              </p>
              <p>
                <Link href={`/opportunities/${alert.event.id}`} className="muted-link">
                  Open event detail
                </Link>
              </p>
            </article>
          ))}
        </div>
      )}
    </DashboardShell>
  );
}
