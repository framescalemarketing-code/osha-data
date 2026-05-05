export type NavView =
  | "overview"
  | "lead-queue"
  | "hot-eye-leads"
  | "ppe-opportunity"
  | "source-signals"
  | "settings";

export type LeadTier = "P0 Hot Eye" | "P1 Eye Violation" | "P2 PPE Opportunity" | "P3 Industry Fit";

/** Legacy compat alias — maps from LeadTier */
export type LeadPriority = "P0 Ideal" | "P1 Active" | "P2 Research" | "P3 Monitor";
export type NeedTier = "Direct Need" | "Probable Need" | "Fit Only";
export type ActionLabel = "Ideal Call Now" | "Call Now" | "Call This Week" | "Research Then Call" | "Monitor / Nurture";
export type IncidentType =
  | "Severe Injury"
  | "Complaint Inspection"
  | "Chemical Exposure"
  | "Prescription Safety"
  | "Fit And Training Gap"
  | "Impact Hazard"
  | "General PPE"
  | "Profile Fit";

export type LeadType = "incident" | "profile_fit";
export type GeoMatchSource = "bay_radius" | "san_diego_area" | "bay_radius|san_diego_area" | "none";

export type IncidentDateSource =
  | "accident"
  | "violation-event"
  | "case-close"
  | "case-open"
  | "unknown";

export type OshaViolationDetail = {
  code: string;
  title: string;
  plainEnglish: string;
  source: "OSHA 1910" | "OSHA 1926" | "OSHA General";
};

export type LeadRecord = {
  id: string;
  company: string;
  region: string;
  county?: string;
  distanceFromMiramarMiles?: number | null;
  bayAreaDistanceMiles?: number | null;
  isWithinBayArea50Mi?: boolean;
  isSanDiegoArea?: boolean;
  geoMatchSource?: GeoMatchSource;
  leadType?: LeadType;
  qualifiesIncident3Year?: boolean;
  city: string;
  industry: string;
  ownerType: string;

  // v3 scores
  eyeLeadScore: number;
  ppeScore: number;
  finalScore: number;
  leadTier: LeadTier;
  pitchRecommendation: string;

  // legacy compat (derived from leadTier in server)
  overallSalesScore: number;
  eyewearEvidenceScore: number;
  priority: LeadPriority;
  needTier: NeedTier;
  action: ActionLabel;

  // eye injury evidence
  eyeInjuryCount: number;
  fatalityCount: number;
  faceHeadInjuryCount: number;
  eyeInjuryDescriptions: string[];

  // violation evidence
  eyeViolationCount: number;
  prescriptionViolationCount: number;
  openEyeViolationCount: number;
  generalPpeViolationCount: number;
  openGeneralPpeViolationCount: number;
  willfulViolationCount: number;
  repeatViolationCount: number;
  totalCurrentPenalty: number;
  rawViolationCodes: string[];
  openViolations: boolean;

  // enrichment signals
  violationEventCount: number;
  contestedViolationCount: number;
  eyeEmphasisCount: number;
  emphasisCodes: string[];
  relatedInspectionCount: number;
  formalFollowupCount: number;
  totalInspectionCount: number;

  // company info
  naicsCode?: string;
  employeeBand: string;

  // dates
  incidentDate: string;
  incidentDateSource?: IncidentDateSource;
  incidentType: IncidentType;
  openCaseDate?: string;
  closeCaseDate?: string;
  lastEyeInjuryDate?: string;

  // legacy compat
  matchedSources: string[];
  reasonToContact: string;
  whyNow: string;
  recentInspectionContext: string;
  severeIncident: boolean;
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
