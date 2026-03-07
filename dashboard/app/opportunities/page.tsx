import Link from "next/link";

import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import {
  filterOpportunityEvents,
  getIntersections,
  getOpportunityEvents,
  uniqueValues,
} from "@/lib/data";

type SearchParams = Record<string, string | string[] | undefined>;

function stringParam(value: string | string[] | undefined): string {
  if (Array.isArray(value)) {
    return value[0] ?? "";
  }
  return value ?? "";
}

export default async function OpportunitiesPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>;
}) {
  const resolvedSearchParams = await searchParams;
  const [events, intersections] = await Promise.all([
    getOpportunityEvents(),
    getIntersections(),
  ]);
  const filtered = filterOpportunityEvents(events, resolvedSearchParams);
  const intersectionCompanies = new Set(
    intersections.map((alert) => alert.companyNameNormalized),
  );

  return (
    <DashboardShell
      eyebrow="RSS Opportunity Feed"
      title="Accepted operational growth events"
    >
      <form className="filter-form">
        <label>
          Industry
          <select name="industry" defaultValue={stringParam(resolvedSearchParams.industry)}>
            <option value="">All</option>
            {uniqueValues(events.map((event) => event.industry)).map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <label>
          Event type
          <select name="eventType" defaultValue={stringParam(resolvedSearchParams.eventType)}>
            <option value="">All</option>
            {uniqueValues(events.map((event) => event.eventType)).map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <label>
          Location
          <input
            name="location"
            defaultValue={stringParam(resolvedSearchParams.location)}
            placeholder="Phoenix, AZ"
          />
        </label>
        <label>
          Score threshold
          <input
            name="score"
            type="number"
            min="0"
            max="100"
            defaultValue={stringParam(resolvedSearchParams.score)}
            placeholder="65"
          />
        </label>
        <label>
          Source
          <select name="source" defaultValue={stringParam(resolvedSearchParams.source)}>
            <option value="">All</option>
            {uniqueValues(events.map((event) => event.sourceName)).map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
        <label>
          Review status
          <select
            name="reviewStatus"
            defaultValue={stringParam(resolvedSearchParams.reviewStatus)}
          >
            <option value="">Accepted only</option>
            <option value="accepted">Accepted</option>
            <option value="review">Review</option>
          </select>
        </label>
        <label>
          Date from
          <input name="dateFrom" type="date" defaultValue={stringParam(resolvedSearchParams.dateFrom)} />
        </label>
        <label>
          Date to
          <input name="dateTo" type="date" defaultValue={stringParam(resolvedSearchParams.dateTo)} />
        </label>
      </form>

      {filtered.length === 0 ? (
        <EmptyState
          title="No accepted opportunities matched the filters"
          body="Adjust the filter set or run the RSS ingestion command to refresh the snapshot."
        />
      ) : (
        <div className="cards-grid">
          {filtered.map((event) => (
            <article key={event.id} className="event-card">
              <div className="row-header">
                <div>
                  <p className="section-kicker">{event.companyName}</p>
                  <h3>
                    <Link href={`/opportunities/${event.id}`}>{event.headline}</Link>
                  </h3>
                </div>
                <span className="pill pill-accent">Score {Math.round(event.eventScore)}</span>
              </div>
              <div className="pill-row">
                <span className="pill">{event.industry}</span>
                <span className="pill">{event.eventType}</span>
                <span className="pill">{event.location || "Location pending"}</span>
                <span className="pill">{event.sourceName}</span>
                {intersectionCompanies.has(event.companyNameNormalized) ? (
                  <span className="pill pill-alert">Intersection alert</span>
                ) : null}
              </div>
              <p>{event.summary}</p>
              <div className="pill-row">
                {event.contacts.slice(0, 4).map((contact) => (
                  <span key={contact.id} className="badge">
                    {contact.contactType}: {contact.contactValue}
                  </span>
                ))}
                {event.contacts.length === 0 ? <span className="badge">No contact path yet</span> : null}
              </div>
              <div className="row-header">
                <span className="subtle">{event.publishedAt || "No publication date"}</span>
                <span className="subtle">{event.reviewStatus}</span>
              </div>
            </article>
          ))}
        </div>
      )}
    </DashboardShell>
  );
}
