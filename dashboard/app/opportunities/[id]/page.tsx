import Link from "next/link";
import { notFound } from "next/navigation";

import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import { getIntersections, getOpportunityEvents, getReviewQueue } from "@/lib/data";

export default async function OpportunityDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const [accepted, reviewQueue, intersections] = await Promise.all([
    getOpportunityEvents(),
    getReviewQueue(),
    getIntersections(),
  ]);
  const event = [...accepted, ...reviewQueue].find((item) => item.id === id);
  if (!event) {
    notFound();
  }

  const relatedAlert = intersections.find(
    (alert) => alert.companyNameNormalized === event.companyNameNormalized,
  );

  return (
    <DashboardShell
      eyebrow="Opportunity Detail"
      title={event.companyName}
    >
      <section className="detail-grid">
        <article>
          <p className="section-kicker">Summary</p>
          <h3>{event.headline}</h3>
          <p>{event.summary}</p>
          <div className="pill-row">
            <span className="pill">{event.industry}</span>
            <span className="pill">{event.eventType}</span>
            <span className="pill pill-accent">Score {Math.round(event.eventScore)}</span>
            <span className="pill">{event.signalStrength}</span>
            <span className="pill">{event.reviewStatus}</span>
          </div>
        </article>
        <article>
          <p className="section-kicker">Classification</p>
          <h3>Why this item was kept</h3>
          <p>{event.classificationReason}</p>
          <div className="pill-row">
            {event.rawKeywords.map((keyword) => (
              <span key={keyword} className="badge">
                {keyword}
              </span>
            ))}
          </div>
          {relatedAlert ? (
            <p className="panel-copy">
              Intersection alert: this company also appears in the current pipeline view.
              <Link href="/intersections" className="muted-link">
                {" "}
                Open alert board
              </Link>
            </p>
          ) : null}
        </article>
        <article>
          <p className="section-kicker">Source links</p>
          <h3>Original reporting path</h3>
          <p>
            <Link href={event.sourceUrl} className="muted-link">
              {event.sourceName}
            </Link>
          </p>
          <p className="subtle">Published: {event.publishedAt || "Unknown"}</p>
          <p className="subtle">Location: {event.location || "Unknown"}</p>
          {event.scaleClues.length > 0 ? (
            <div className="pill-row">
              {event.scaleClues.map((clue) => (
                <span key={clue} className="badge">
                  {clue}
                </span>
              ))}
            </div>
          ) : null}
        </article>
        <article>
          <p className="section-kicker">Review notes</p>
          <h3>Seller notes placeholder</h3>
          <div className="notes-placeholder">
            Capture outreach angle, site assumptions, and whether the intersection alert should change follow-up timing.
          </div>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="section-kicker">Contact paths</p>
            <h3>Any public route to the company</h3>
          </div>
        </div>
        {event.contacts.length === 0 ? (
          <EmptyState
            title="No contact path extracted yet"
            body="This event was accepted, but the enrichment step did not find a public company route on the source or linked site."
          />
        ) : (
          <div className="contact-grid">
            {event.contacts.map((contact) => (
              <article key={contact.id} className="event-card">
                <div className="pill-row">
                  <span className="pill">{contact.contactType}</span>
                  {contact.title ? <span className="pill">{contact.title}</span> : null}
                </div>
                <h3>{contact.name || contact.contactValue}</h3>
                {contact.name ? <p>{contact.contactValue}</p> : null}
                <Link href={contact.sourceUrl} className="muted-link">
                  {contact.sourceName}
                </Link>
              </article>
            ))}
          </div>
        )}
      </section>
    </DashboardShell>
  );
}
