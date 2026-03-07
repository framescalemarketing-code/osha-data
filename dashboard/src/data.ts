import { startTransition } from "react";

import type { PublicSnapshot, RssSnapshot } from "./types";

async function loadJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function loadRssSnapshot(
  setState: (state: { data: RssSnapshot | null; error: string | null }) => void,
): Promise<void> {
  try {
    const data = await loadJson<RssSnapshot>("/data/rss-feed.json");
    startTransition(() => setState({ data, error: null }));
  } catch (error) {
    startTransition(() =>
      setState({
        data: null,
        error: error instanceof Error ? error.message : "Unknown RSS load error",
      }),
    );
  }
}

export async function loadPublicSnapshot(
  setState: (state: { data: PublicSnapshot | null; error: string | null }) => void,
): Promise<void> {
  try {
    const data = await loadJson<PublicSnapshot>("/data/public-sources.json");
    startTransition(() => setState({ data, error: null }));
  } catch (error) {
    startTransition(() =>
      setState({
        data: null,
        error: error instanceof Error ? error.message : "Unknown public-sources load error",
      }),
    );
  }
}
