export type NavView =
  | "overview"
  | "lead-queue"
  | "hot-accounts"
  | "research-needed"
  | "source-signals"
  | "saved-views"
  | "settings";

export type LeadPriority = "P0 Ideal" | "P1 Active" | "P2 Research" | "P3 Monitor";
export type NeedTier = "Direct Need" | "Probable Need" | "Fit Only";
export type ActionLabel = "Call Now" | "Call This Week" | "Research Then Call" | "Monitor / Nurture";
export type IncidentType =
  | "Severe Injury"
  | "Complaint Inspection"
  | "Chemical Exposure"
  | "Prescription Safety"
  | "Fit And Training Gap"
  | "Impact Hazard"
  | "General PPE";

export type OshaViolationDetail = {
  code: string;
  title: string;
  plainEnglish: string;
  source: "OSHA 1910" | "OSHA 1926" | "OSHA General";
};

export type LeadRecord = {
  id: string;
  company: string;
  region: "San Diego" | "Bay Area";
  city: string;
  industry: string;
  ownerType: string;
  overallSalesScore: number;
  eyewearEvidenceScore: number;
  priority: LeadPriority;
  needTier: NeedTier;
  action: ActionLabel;
  matchedSources: string[];
  reasonToContact: string;
  whyNow: string;
  recentInspectionContext: string;
  incidentDate: string;
  incidentType: IncidentType;
  rawViolationCodes: string[];
  openViolations: boolean;
  severeIncident: boolean;
  employeeBand: string;
  lastTouchedDays: number;
  accountStatus: "New" | "In Review" | "Contacted";
  outreachStatus?: "new" | "attempted" | "connected" | "won" | "lost";
  outreachNotes?: string;
  outreachUpdatedAt?: string;
};

export type DashboardSettings = {
  compactCards: boolean;
  showOnlyContactReady: boolean;
  themeName: "signal" | "neutral";
};
