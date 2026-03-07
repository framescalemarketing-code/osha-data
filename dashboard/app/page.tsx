import Link from "next/link";

import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import { MetricCard } from "@/components/metric-card";
import {
  getCurrentSignals,
  getIntersections,
  getOpportunityEvents,
  getSummary,
} from "@/lib/data";

export default async function OverviewPage() {
  const [summary, currentSignals, opportunities, intersections] = await Promise.all([
    getSummary(),
    getCurrentSignals(),
    getOpportunityEvents(),
    getIntersections(),
  ]);

  return (
    <DashboardShell
      eyebrow="Unified Sales Intelligence"
      title="Current repo signals plus RSS opportunity expansion alerts"
    >
      <section className="metrics-grid">
        <MetricCard label="Current signal rows" value={summary.currentSignalCount} accent="blue" />
        <MetricCard label="Accepted RSS opportunities" value={summary.opportunityEventCount} accent="amber" />
        <MetricCard label="Intersection alerts" value={summary.intersectionCount} accent="red" />
        <MetricCard label="Review queue" value={summary.reviewQueueCount} accent="green" />
      </section>

      <section className="two-column">
        <article className="panel">
          <p className="section-kicker">Current pipeline</p>
          <h3>Sales-ready rows from the existing repo</h3>
          <p className="panel-copy">
            This view keeps the current scoring and language intact, then adds alerting when a company also appears in RSS expansion coverage.
          </p>
          <div className="badge-row">
            {currentSignals.slice(0, 5).map((signal) => (
              <span key={signal.id} className="badge">
                {signal.companyName} · {signal.currentPriority || "No priority"}
              </span>
            ))}
          </div>
          <p>
            <Link href="/current" className="muted-link">
              Open current signals
            </Link>
          </p>
        </article>
        <article className="panel">
          <p className="section-kicker">RSS opportunities</p>
          <h3>Operational growth events only</h3>
          <p className="panel-copy">
            Feed items are filtered down to facility, construction, capacity, warehouse, and workforce growth signals where PPE eyewear demand is more likely.
          </p>
          <div className="badge-row">
            {opportunities.slice(0, 5).map((event) => (
              <span key={event.id} className="badge">
                {event.companyName} · {event.eventType}
              </span>
            ))}
          </div>
          <p>
            <Link href="/opportunities" className="muted-link">
              Open RSS opportunities
            </Link>
          </p>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="section-kicker">Intersections</p>
            <h3>Companies appearing in both channels</h3>
          </div>
          <Link href="/intersections" className="muted-link">
            See all alerts
          </Link>
        </div>
        {intersections.length === 0 ? (
          <EmptyState
            title="No intersections yet"
            body="Run the current-sales sync and the RSS ingestion job to populate alert rows here."
          />
        ) : (
          <div className="cards-grid">
            {intersections.slice(0, 4).map((alert) => (
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
                  <span className="pill">{alert.currentSignal.currentPriority}</span>
                  <span className="pill pill-accent">{alert.event.eventType}</span>
                </div>
                <p className="panel-copy">{alert.alertReason}</p>
              </article>
            ))}
          </div>
        )}
      </section>
    </DashboardShell>
  );
}
