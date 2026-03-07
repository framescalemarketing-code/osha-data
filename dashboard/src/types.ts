export type RssWatchlistItem = {
  "Account Name": string;
  "Region": string;
  "Eyewear Need Tier": string;
  "Article Title": string;
  "Article Link": string;
  "Feed Title": string;
  "Article Priority": string;
  "Article Eyewear Relevance Score": number;
  "Article Urgency Score": number;
};

export type RssArticleItem = {
  feed_title: string;
  article_title: string;
  article_link: string;
  article_priority: string;
  eyewear_relevance_score: number;
  urgency_score: number;
  article_published_at: string;
};

export type RssSnapshot = {
  exported_at: string;
  article_count: number;
  watchlist_count: number;
  watchlist: RssWatchlistItem[];
  articles: RssArticleItem[];
};

export type PublicSourceFreshness = {
  source_name: string;
  record_count: number;
  latest_timestamp: string;
};

export type PublicNaicsItem = {
  naics2: string;
  establishments_ca: number;
  employees_ca: number;
  annual_payroll_ca: number;
  federal_amount_ca: number;
  external_signal_points: number;
};

export type BlsGrowthItem = {
  segment: string;
  latest_value: number;
  prior_12m_value: number;
  pct_change_12m: number | null;
};

export type PublicSnapshot = {
  exported_at: string;
  source_freshness: PublicSourceFreshness[];
  naics_enrichment: PublicNaicsItem[];
  bls_growth: BlsGrowthItem[];
};
