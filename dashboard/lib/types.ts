export type Summary = {
  generatedAt: string;
  currentSignalCount: number;
  opportunityEventCount: number;
  reviewQueueCount: number;
  intersectionCount: number;
  highPriorityCurrentCount: number;
  highOpportunityCount: number;
};

export type CurrentSignal = {
  id: string;
  companyName: string;
  companyNameNormalized: string;
  region: string;
  siteAddress: string;
  siteCity: string;
  siteState: string;
  siteZip: string;
  industrySegment: string;
  currentPriority: string;
  currentAction: string;
  overallSalesScore: number;
  matchedSources: string;
  reasonToContact: string;
  reasonToCallNow: string;
  whyFit: string;
  whyNow: string;
  rawPayload: Record<string, unknown>;
};

export type ContactPath = {
  id: string;
  contactType: string;
  contactValue: string;
  name: string;
  title: string;
  sourceName: string;
  sourceUrl: string;
};

export type OpportunityEvent = {
  id: string;
  companyName: string;
  companyNameNormalized: string;
  industry: string;
  eventType: string;
  headline: string;
  summary: string;
  sourceName: string;
  sourceUrl: string;
  publishedAt: string;
  location: string;
  eventScore: number;
  signalStrength: string;
  reviewStatus: string;
  rawKeywords: string[];
  scaleClues: string[];
  classificationReason: string;
  contacts: ContactPath[];
  rawPayload: Record<string, unknown>;
};

export type IntersectionAlert = {
  id: string;
  companyName: string;
  companyNameNormalized: string;
  alertScore: number;
  alertReason: string;
  updatedAt: string;
  currentSignal: {
    id: string;
    region: string;
    currentPriority: string;
    currentAction: string;
    overallSalesScore: number;
    matchedSources: string;
  };
  event: {
    id: string;
    headline: string;
    eventType: string;
    industry: string;
    location: string;
    eventScore: number;
    sourceName: string;
  };
};
