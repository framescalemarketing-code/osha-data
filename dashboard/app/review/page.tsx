import Link from "next/link";

import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import { getReviewQueue } from "@/lib/data";

export default async function ReviewQueuePage() {
  const reviewQueue = await getReviewQueue();

  return (
    <DashboardShell
      eyebrow="Review Queue"
      title="Borderline RSS items for manual review"
    >
      {reviewQueue.length === 0 ? (
        <EmptyState
          title="Review queue is empty"
          body="Only borderline items land here. Accepted events stay on the main RSS opportunities page."
        />
      ) : (
        <div className="cards-grid">
          {reviewQueue.map((event) => (
            <article key={event.id} className="event-card">
              <div className="row-header">
                <div>
                  <p className="section-kicker">{event.companyName}</p>
                  <h3>
                    <Link href={`/opportunities/${event.id}`}>{event.headline}</Link>
                  </h3>
                </div>
                <span className="pill">{Math.round(event.eventScore)}</span>
              </div>
              <div className="pill-row">
                <span className="pill">{event.eventType}</span>
                <span className="pill">{event.industry}</span>
                <span className="pill">{event.location || "Unknown location"}</span>
              </div>
              <p>{event.classificationReason}</p>
            </article>
          ))}
        </div>
      )}
    </DashboardShell>
  );
}
