import { promises as fs } from "fs";
import path from "path";

import type {
  CurrentSignal,
  IntersectionAlert,
  OpportunityEvent,
  Summary,
} from "@/lib/types";

const SNAPSHOT_DIR = path.resolve(process.cwd(), "..", "data", "dashboard");

type SearchParams = Record<string, string | string[] | undefined>;

async function readSnapshot<T>(filename: string, fallback: T): Promise<T> {
  try {
    const raw = await fs.readFile(path.join(SNAPSHOT_DIR, filename), "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function paramValue(value: string | string[] | undefined): string {
  if (Array.isArray(value)) {
    return value[0] ?? "";
  }
  return value ?? "";
}

function includesCI(haystack: string, needle: string): boolean {
  return haystack.toLowerCase().includes(needle.toLowerCase());
}

export async function getSummary(): Promise<Summary> {
  return readSnapshot<Summary>("summary.json", {
    generatedAt: "",
    currentSignalCount: 0,
    opportunityEventCount: 0,
    reviewQueueCount: 0,
    intersectionCount: 0,
    highPriorityCurrentCount: 0,
    highOpportunityCount: 0,
  });
}

export async function getCurrentSignals(): Promise<CurrentSignal[]> {
  return readSnapshot<CurrentSignal[]>("current-signals.json", []);
}

export async function getOpportunityEvents(): Promise<OpportunityEvent[]> {
  return readSnapshot<OpportunityEvent[]>("opportunity-events.json", []);
}

export async function getReviewQueue(): Promise<OpportunityEvent[]> {
  return readSnapshot<OpportunityEvent[]>("review-queue.json", []);
}

export async function getIntersections(): Promise<IntersectionAlert[]> {
  return readSnapshot<IntersectionAlert[]>("intersections.json", []);
}

export function filterOpportunityEvents(
  events: OpportunityEvent[],
  searchParams: SearchParams,
): OpportunityEvent[] {
  const industry = paramValue(searchParams.industry);
  const eventType = paramValue(searchParams.eventType);
  const location = paramValue(searchParams.location);
  const scoreThreshold = Number(paramValue(searchParams.score) || "0");
  const source = paramValue(searchParams.source);
  const reviewStatus = paramValue(searchParams.reviewStatus);
  const dateFrom = paramValue(searchParams.dateFrom);
  const dateTo = paramValue(searchParams.dateTo);

  return events.filter((event) => {
    if (industry && event.industry !== industry) {
      return false;
    }
    if (eventType && event.eventType !== eventType) {
      return false;
    }
    if (location && !includesCI(event.location, location)) {
      return false;
    }
    if (scoreThreshold && event.eventScore < scoreThreshold) {
      return false;
    }
    if (source && event.sourceName !== source) {
      return false;
    }
    if (reviewStatus && event.reviewStatus !== reviewStatus) {
      return false;
    }
    if (dateFrom && event.publishedAt && event.publishedAt < dateFrom) {
      return false;
    }
    if (dateTo && event.publishedAt && event.publishedAt > dateTo) {
      return false;
    }
    return true;
  });
}

export function uniqueValues(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean))).sort((left, right) =>
    left.localeCompare(right),
  );
}
