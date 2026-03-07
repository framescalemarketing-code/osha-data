export type RssAlignmentItem = {
  "Account Name": string;
  "Region": string;
  "Eyewear Need Tier": string;
  "Article Title": string;
  "Article Link": string;
  "Feed Title": string;
  "Article Priority": string;
  "Article Opportunity Signal Score": number;
  "Article Momentum Score": number;
  "Article Signal Summary": string;
};

export type RssArticleItem = {
  feed_title: string;
  article_title: string;
  article_link: string;
  article_priority: string;
  opportunity_signal_score: number;
  momentum_score: number;
  signal_summary: string;
  article_published_at: string;
};

export type RssSnapshot = {
  exported_at: string;
  article_count: number;
  alignment_watchlist_count: number;
  alignment_watchlist: RssAlignmentItem[];
  articles: RssArticleItem[];
};

export type PublicSourceFreshness = {
  source_name: string;
  record_count: number;
  latest_timestamp: string;
};

export type PublicNaicsItem = {
  region: string;
  industry_segment: string;
  naics2: string;
  account_count: number;
  priority_1_count: number;
  establishments_ca: number;
  employees_ca: number;
  annual_payroll_ca: number;
  federal_amount_ca: number;
  external_signal_points: number;
};

export type LocalTargetSummaryItem = {
  region: string;
  naics2: string;
  industry_segment: string;
  account_count: number;
  priority_1_count: number;
};

export type BlsGrowthItem = {
  segment: string;
  latest_value: number;
  prior_12m_value: number;
  pct_change_12m: number | null;
};

export type PublicSnapshot = {
  exported_at: string;
  regions: string[];
  source_freshness: PublicSourceFreshness[];
  local_target_summary: LocalTargetSummaryItem[];
  naics_enrichment: PublicNaicsItem[];
  bls_growth: BlsGrowthItem[];
  top_accounts?: PublicAccountItem[];
};

export type PublicAccountItem = {
  account_name: string;
  region: string;
  naics2: string;
  industry_segment: string;
  overall_sales_priority?: string;
  site_address?: string;
  site_city?: string;
  site_zip?: string;
  ownership_type?: string;
  organization_class?: string;
  recent_inspection_context?: string;
  overall_history?: string;
  reason_to_contact?: string;
};
