import * as React from "react";
import {
  Alert,
  alpha,
  AppBar,
  Avatar,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Drawer,
  FormControl,
  Grid,
  IconButton,
  InputAdornment,
  InputLabel,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  MenuItem,
  Select,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Toolbar,
  Tooltip,
  Typography,
} from "@mui/material";
import AssessmentRoundedIcon from "@mui/icons-material/AssessmentRounded";
import AutoAwesomeRoundedIcon from "@mui/icons-material/AutoAwesomeRounded";
import BlockRoundedIcon from "@mui/icons-material/BlockRounded";
import ChevronLeftRoundedIcon from "@mui/icons-material/ChevronLeftRounded";
import ChevronRightRoundedIcon from "@mui/icons-material/ChevronRightRounded";
import FilterAltRoundedIcon from "@mui/icons-material/FilterAltRounded";
import FlagRoundedIcon from "@mui/icons-material/FlagRounded";
import LocalFireDepartmentRoundedIcon from "@mui/icons-material/LocalFireDepartmentRounded";
import MenuRoundedIcon from "@mui/icons-material/MenuRounded";
import RefreshRoundedIcon from "@mui/icons-material/RefreshRounded";
import SearchRoundedIcon from "@mui/icons-material/SearchRounded";
import SettingsRoundedIcon from "@mui/icons-material/SettingsRounded";
import SourceRoundedIcon from "@mui/icons-material/SourceRounded";
import TuneRoundedIcon from "@mui/icons-material/TuneRounded";
import UndoRoundedIcon from "@mui/icons-material/UndoRounded";
import { leads as fallbackLeads } from "./data";
import { toViolationDetails } from "./oshaStandards";
import type { DashboardSettings, IncidentDateSource, IncidentType, LeadRecord, LeadTier, NavView } from "./types";

const drawerWidthExpanded = 300;
const drawerWidthCollapsed = 88;

const initialSettings: DashboardSettings = {
  compactCards: false,
  showOnlyContactReady: false,
  themeName: "signal",
};

type PullStatus = {
  id: string;
  mode?: "refresh" | "full";
  status: "running" | "success" | "failed";
  startedAt: string;
  endedAt?: string;
  durationSeconds?: number;
  error?: string;
};

type PullHistoryItem = {
  id: string;
  status: "running" | "success" | "failed";
  startedAt: string;
  endedAt: string | null;
  durationSeconds: number | null;
  message: string;
};

const navItems: Array<{ view: NavView; label: string; icon: React.ReactNode }> = [
  { view: "overview", label: "Overview", icon: <AssessmentRoundedIcon /> },
  { view: "lead-queue", label: "Lead Queue", icon: <FilterAltRoundedIcon /> },
  { view: "hot-eye-leads", label: "Hot Eye Leads", icon: <LocalFireDepartmentRoundedIcon /> },
  { view: "ppe-opportunity", label: "PPE Opportunity", icon: <FlagRoundedIcon /> },
  { view: "source-signals", label: "Source Signals", icon: <SourceRoundedIcon /> },
  { view: "settings", label: "Settings", icon: <SettingsRoundedIcon /> },
];

const incidentOptions: IncidentType[] = [
  "Severe Injury",
  "Complaint Inspection",
  "Chemical Exposure",
  "Prescription Safety",
  "Fit And Training Gap",
  "Impact Hazard",
  "General PPE",
  "Profile Fit",
];

const CONTACT_READY_ACTIONS: LeadRecord["action"][] = [
  "Ideal Call Now",
  "Call Now",
  "Call This Week",
];

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];
const DEFAULT_PAGE_SIZE = 25;
const BAD_LEADS_STORAGE_KEY = "osha_dashboard_bad_leads_v1";
const NAICS_RULES_STORAGE_KEY = "osha_naics_rules_v1";
const OUTCOMES_STORAGE_KEY = "osha_dashboard_outcomes_v1";
const NAMED_ACCOUNTS_KEY = "osha_named_accounts_v1";

// ---------------------------------------------------------------------------
// Named / known accounts — companies already in your pipeline
// ---------------------------------------------------------------------------
type AccountListStatus = "cold" | "warm" | "hot" | "meeting" | "not_interested" | "unqualified";
type NamedAccountEntry = { displayName: string; status: AccountListStatus; addedAt: string };

const ACCOUNT_STATUS_LABELS: Record<AccountListStatus, string> = {
  cold: "Cold",
  warm: "Warm",
  hot: "Hot / Research",
  meeting: "Meeting Scheduled",
  not_interested: "Not Interested",
  unqualified: "Unqualified",
};

const ACCOUNT_STATUS_COLORS: Record<AccountListStatus, "default" | "primary" | "warning" | "success" | "error" | "info"> = {
  cold: "default",
  warm: "warning",
  hot: "info",
  meeting: "success",
  not_interested: "error",
  unqualified: "error",
};

// Pre-seeded from sales team's account list (May 2026)
// Keyed by normalizeCompanyKey(displayName)
const NAMED_ACCOUNTS_SEED: Record<string, NamedAccountEntry> = (() => {
  const seed: [string, AccountListStatus][] = [
    // Cold
    ["Lawrence Livermore National Laboratory", "cold"],
    ["Lithium Americas", "cold"],
    ["Seaworld", "cold"],
    ["Archer Aviation", "cold"],
    ["Maravai LifeSciences", "cold"],
    ["ThermoFisher", "cold"],
    ["Thermo Fisher Scientific", "cold"],
    ["City of Escondido", "cold"],
    ["City of Coronado", "cold"],
    ["Exagen", "cold"],
    ["Charles River", "cold"],
    ["Axillon Aerospace", "cold"],
    ["Aventus", "cold"],
    ["Genentech", "cold"],
    ["Qualcomm", "cold"],
    ["Halozyme", "cold"],
    ["Tesla", "cold"],
    ["National Indian Gaming Commission", "cold"],
    ["Verdiam", "cold"],
    ["V2X Inc", "cold"],
    ["V2X", "cold"],
    ["ACTenviro", "cold"],
    ["Dudek", "cold"],
    ["UCSD", "cold"],
    ["Takeda", "cold"],
    ["Sangamo Therapeutics", "cold"],
    ["BeautyHealth", "cold"],
    ["Nitto Avecia", "cold"],
    ["ITT Inc", "cold"],
    ["Federal Aviation Administration", "cold"],
    ["UPSIDE Food", "cold"],
    ["ERM", "cold"],
    ["Sodexo", "cold"],
    ["Edwards Lifesciences", "cold"],
    ["Edward Lifescience", "cold"],
    ["TE Connectivity", "cold"],
    ["Bechtel", "cold"],
    // Warm
    ["AP Precision", "warm"],
    ["Capstone Fire Safety Management", "warm"],
    ["Bristol Myers Squibb", "warm"],
    ["Technical Safety Services", "warm"],
    ["Technical Safety Service", "warm"],
    ["DYE CNC", "warm"],
    ["Padre Dam Municipal Water District", "warm"],
    ["Pacific Rim Mechanical", "warm"],
    ["Active Motif", "warm"],
    ["Collins Aerospace", "warm"],
    ["Eclipse Bio", "warm"],
    ["Bachem", "warm"],
    ["Terumo Neuro", "warm"],
    ["Krieger Specialty Products", "warm"],
    ["Jurupa Community Services District", "warm"],
    // Hot / Research
    ["CalPortland", "hot"],
    ["Digital Building Components", "hot"],
    // Meeting Scheduled
    ["Rainbow OMWD", "meeting"],
    ["Neurocrine Biosciences", "meeting"],
    ["Automated Engineering Services", "meeting"],
    ["Form Energy", "meeting"],
    ["Fisica Simi Valley", "meeting"],
    ["VLS Environmental Solutions", "meeting"],
    // Not Interested — DO NOT CONTACT
    ["Bio-Edge", "not_interested"],
    ["Gilead Sciences", "not_interested"],
    ["Amentum", "not_interested"],
    ["Northrop Grumman", "not_interested"],
    ["Berkeley Lab", "not_interested"],
    ["Lawrence Berkeley National Laboratory", "not_interested"],
    ["Qnity", "not_interested"],
    ["Hoffmaster", "not_interested"],
    ["ASML", "not_interested"],
    ["PacBio", "not_interested"],
    ["Pacific Biosciences", "not_interested"],
    ["Apple", "not_interested"],
    ["Sutro Biopharma", "not_interested"],
    ["Metalfx", "not_interested"],
    ["CBRE", "not_interested"],
    ["Arcadias", "not_interested"],
    ["Inhibrx", "not_interested"],
    ["Abbott", "not_interested"],
    ["Lam Research", "not_interested"],
    ["Recludix Pharma", "not_interested"],
    ["Boundless Bio", "not_interested"],
    ["Amazon", "not_interested"],
    ["Vera Therapeutics", "not_interested"],
    ["Amyris", "not_interested"],
    ["CW Services", "not_interested"],
    ["Revolution Medicines", "not_interested"],
    ["General Atomics", "not_interested"],
    ["Ultragenyx", "not_interested"],
    ["Abbvie", "not_interested"],
    ["AbbVie", "not_interested"],
    ["Lyten", "not_interested"],
    ["Millennium Health", "not_interested"],
    ["Hologic", "not_interested"],
    ["Penumbra", "not_interested"],
    ["Jabil", "not_interested"],
    ["Teknova", "not_interested"],
    ["UC San Diego", "not_interested"],
    ["University of California San Diego", "not_interested"],
    ["Bio-Rad Laboratories", "not_interested"],
    ["Bio-Rad", "not_interested"],
    ["Personalis", "not_interested"],
    ["Veracyte", "not_interested"],
    ["Verily", "not_interested"],
    ["Bender CCP", "not_interested"],
    ["Veolia North America", "not_interested"],
    ["Veolia", "not_interested"],
    ["Preson Infrastructure", "not_interested"],
    ["Solar Turbines", "not_interested"],
    ["Santier", "not_interested"],
    ["ASLM", "not_interested"],
    ["San Diego Padres", "not_interested"],
    ["EMCORE Corporation", "not_interested"],
    ["EMCORE", "not_interested"],
    ["FairJourney Bio", "not_interested"],
    ["Grifols", "not_interested"],
    ["Grifol", "not_interested"],
    ["UPSIDE Foods", "not_interested"],
    ["Toray Membrane USA", "not_interested"],
    ["Cemex", "not_interested"],
    ["Baxter International", "not_interested"],
    ["Baxter", "not_interested"],
    ["CP Kelco", "not_interested"],
    // Unqualified — DO NOT CONTACT
    ["Kannegiesser", "unqualified"],
    ["Stone Brewing", "unqualified"],
    ["National Tech", "unqualified"],
    ["Promach", "unqualified"],
  ];
  const now = new Date().toISOString();
  const result: Record<string, NamedAccountEntry> = {};
  for (const [displayName, status] of seed) {
    const key = displayName.replace(/[^A-Z0-9]/gi, "").toUpperCase();
    result[key] = { displayName, status, addedAt: now };
  }
  return result;
})();

function normalizeCompanyKey(name: string): string {
  return name.replace(/[^A-Z0-9]/gi, "").toUpperCase();
}

function loadNamedAccounts(): Record<string, NamedAccountEntry> {
  try {
    const raw = localStorage.getItem(NAMED_ACCOUNTS_KEY);
    if (!raw) {
      // First load — write seed and return it
      localStorage.setItem(NAMED_ACCOUNTS_KEY, JSON.stringify(NAMED_ACCOUNTS_SEED));
      return { ...NAMED_ACCOUNTS_SEED };
    }
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ...NAMED_ACCOUNTS_SEED };
    }
    // Merge seed entries that aren't already present
    const merged = { ...parsed };
    for (const [k, v] of Object.entries(NAMED_ACCOUNTS_SEED)) {
      if (!merged[k]) merged[k] = v;
    }
    return merged;
  } catch {
    return { ...NAMED_ACCOUNTS_SEED };
  }
}

function saveNamedAccounts(accounts: Record<string, NamedAccountEntry>): void {
  try {
    localStorage.setItem(NAMED_ACCOUNTS_KEY, JSON.stringify(accounts));
  } catch {}
}

type BadLeadReason =
  | "wrong_industry"
  | "consumer_business"
  | "out_of_business"
  | "too_small"
  | "duplicate"
  | "other";

const BAD_LEAD_REASON_LABELS: Record<BadLeadReason, string> = {
  wrong_industry: "Wrong industry (e.g., salon, restaurant, retail)",
  consumer_business: "Consumer business — not B2B",
  out_of_business: "No longer in business",
  too_small: "Too small / sole proprietor",
  duplicate: "Duplicate of another lead",
  other: "Other",
};

// Reasons that should generate a broad NAICS suppression rule (suppress the
// entire 4-digit NAICS subsector so similar companies are auto-filtered).
const BROAD_SUPPRESS_REASONS: BadLeadReason[] = ["wrong_industry", "consumer_business"];

type NaicsRule = {
  prefix: string;    // 4-digit NAICS prefix (e.g. "7225") or 6-digit for narrow suppression
  label: string;     // human-readable industry label
  reason: BadLeadReason;
  exampleCompany: string;
  addedAt: string;
};

type BadLeadEntry = {
  leadId: string;
  company: string;
  industry: string;
  city: string;
  region: string;
  naicsCode?: string;
  leadTier: string;
  reason: BadLeadReason;
  markedAt: string;
};

function loadBadLeads(): BadLeadEntry[] {
  try {
    const raw = localStorage.getItem(BAD_LEADS_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveBadLeads(entries: BadLeadEntry[]): void {
  try {
    localStorage.setItem(BAD_LEADS_STORAGE_KEY, JSON.stringify(entries));
  } catch {
    // Storage quota — silently ignore
  }
}

function loadNaicsRules(): NaicsRule[] {
  try {
    const raw = localStorage.getItem(NAICS_RULES_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveNaicsRules(rules: NaicsRule[]): void {
  try {
    localStorage.setItem(NAICS_RULES_STORAGE_KEY, JSON.stringify(rules));
  } catch {}
}

type OutcomeEntry = {
  outreachStatus: OutreachStatus;
  outreachNotes: string;
  outreachUpdatedAt: string;
  accountStatus: "New" | "In Review" | "Contacted";
};

function loadOutcomes(): Record<string, OutcomeEntry> {
  try {
    const raw = localStorage.getItem(OUTCOMES_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function saveOutcomes(map: Record<string, OutcomeEntry>): void {
  try {
    localStorage.setItem(OUTCOMES_STORAGE_KEY, JSON.stringify(map));
  } catch {}
}

/** Merge locally-stored outcomes onto an API lead list so saves survive cold starts. */
function applyLocalOutcomes(leads: LeadRecord[], outcomes: Record<string, OutcomeEntry>): LeadRecord[] {
  if (Object.keys(outcomes).length === 0) return leads;
  return leads.map((lead) => {
    const local = outcomes[lead.id];
    if (!local) return lead;
    return {
      ...lead,
      outreachStatus: local.outreachStatus,
      outreachNotes: local.outreachNotes,
      outreachUpdatedAt: local.outreachUpdatedAt,
      accountStatus: local.accountStatus,
    };
  });
}

function matchesSearch(lead: LeadRecord, query: string) {
  if (!query.trim()) {
    return true;
  }

  const normalizedViolations = toViolationDetails(lead.rawViolationCodes);
  const haystack = [
    lead.company,
    lead.city,
    lead.region,
    lead.county || "",
    lead.industry,
    lead.leadTier,
    lead.pitchRecommendation,
    lead.incidentType,
    lead.incidentDate,
    lead.action,
    lead.emphasisCodes?.join(" ") ?? "",
    lead.eyeInjuryDescriptions?.join(" ") ?? "",
    lead.rawViolationCodes.join(" "),
    normalizedViolations.map((item) => `${item.title} ${item.plainEnglish}`).join(" "),
  ]
    .join(" ")
    .toLowerCase();

  return haystack.includes(query.trim().toLowerCase());
}

function getTierColor(tier: LeadTier): { bg: string; text: string; chipColor: "error" | "warning" | "info" | "default" } {
  switch (tier) {
    case "P0 Hot Eye":
      return { bg: "#fef2f2", text: "#991b1b", chipColor: "error" };
    case "P1 Eye Violation":
      return { bg: "#fff7ed", text: "#9a3412", chipColor: "warning" };
    case "P2 PPE Opportunity":
      return { bg: "#fefce8", text: "#713f12", chipColor: "info" };
    default:
      return { bg: "#f8fafc", text: "#475569", chipColor: "default" };
  }
}

function getTierLabel(tier: LeadTier): string {
  switch (tier) {
    case "P0 Hot Eye":
      return "🔴 Hot Eye Lead";
    case "P1 Eye Violation":
      return "🟠 Eye Violation";
    case "P2 PPE Opportunity":
      return "🟡 PPE Opportunity";
    default:
      return "⚪ Industry Fit";
  }
}

function getTierLabelForLead(lead: LeadRecord): string {
  // City-license leads can be elevated by industry fit scoring without OSHA incident evidence.
  if (
    lead.leadTier === "P1 Eye Violation"
    && lead.eyeViolationCount <= 0
    && lead.eyeInjuryCount <= 0
    && lead.prescriptionViolationCount <= 0
    && !isTrueIncidentSource(lead.incidentDateSource)
  ) {
    return "🟠 High Hazard Fit";
  }

  return getTierLabel(lead.leadTier ?? "P3 Industry Fit");
}

function getDistanceLabelForLead(lead: LeadRecord): string {
  const bay = lead.bayAreaDistanceMiles;
  const sanDiego = lead.distanceFromMiramarMiles;
  const geo = lead.geoMatchSource || "none";

  if (geo === "bay_radius" && bay != null) {
    return `${bay.toFixed(1)} mi from 16440 Ashland Ave (San Lorenzo)`;
  }

  if (geo === "san_diego_area" && sanDiego != null) {
    return `${sanDiego.toFixed(1)} mi from San Diego anchor`;
  }

  if (geo === "bay_radius|san_diego_area") {
    if (bay != null && sanDiego != null) {
      if (bay <= sanDiego) {
        return `${bay.toFixed(1)} mi from 16440 Ashland Ave (San Lorenzo)`;
      }
      return `${sanDiego.toFixed(1)} mi from San Diego anchor`;
    }
    if (bay != null) {
      return `${bay.toFixed(1)} mi from 16440 Ashland Ave (San Lorenzo)`;
    }
    if (sanDiego != null) {
      return `${sanDiego.toFixed(1)} mi from San Diego anchor`;
    }
  }

  if (lead.isWithinBayArea50Mi && bay != null) {
    return `${bay.toFixed(1)} mi from 16440 Ashland Ave (San Lorenzo)`;
  }

  if (lead.isSanDiegoArea && sanDiego != null) {
    return `${sanDiego.toFixed(1)} mi from San Diego anchor`;
  }

  return "";
}

function getPriorityTone(priority: LeadRecord["priority"]) {
  switch (priority) {
    case "P0 Ideal":
      return "error";
    case "P1 Active":
      return "warning";
    case "P2 Research":
      return "primary";
    default:
      return "default";
  }
}

function getActionTone(action: LeadRecord["action"]) {
  switch (action) {
    case "Ideal Call Now":
    case "Call Now":
      return "error";
    case "Call This Week":
      return "warning";
    case "Research Then Call":
      return "primary";
    default:
      return "default";
  }
}

function StatCard({
  label,
  value,
  supporting,
}: {
  label: string;
  value: string;
  supporting: string;
}) {
  return (
    <Card sx={{ height: "100%" }}>
      <CardContent>
        <Typography color="text.secondary" variant="overline">
          {label}
        </Typography>
        <Typography sx={{ mt: 1 }} variant="h4">
          {value}
        </Typography>
        <Typography sx={{ mt: 1 }} color="text.secondary" variant="body2">
          {supporting}
        </Typography>
      </CardContent>
    </Card>
  );
}

function LeadCard({ lead, compact }: { lead: LeadRecord; compact: boolean }) {
  const normalizedViolations = toViolationDetails(lead.rawViolationCodes);
  const tier = lead.leadTier ?? "P3 Industry Fit";
  const tierStyle = getTierColor(tier);
  const pad = compact ? 2 : 2.5;
  const distanceLabel = getDistanceLabelForLead(lead);

  const compactChipSx = {
    height: 24,
    maxWidth: "100%",
    "& .MuiChip-label": {
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
  };

  return (
    <Card
      sx={{
        borderTop: `3px solid ${tierStyle.text}`,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* ── HEADER ── */}
      <Box sx={{ bgcolor: tierStyle.bg, px: pad, pt: pad, pb: 1.25 }}>
        <Stack direction="row" justifyContent="space-between" alignItems="flex-start" spacing={1.5}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="h6" sx={{ fontSize: "1.1rem", fontWeight: 700, lineHeight: 1.2 }} noWrap>
              {lead.company}
            </Typography>
            <Typography color="text.secondary" variant="caption" sx={{ display: "block", lineHeight: 1.3 }}>
              {lead.city}
              {lead.county ? `, ${lead.county} County` : ""} · {lead.region}
              {distanceLabel ? ` · ${distanceLabel}` : ""}
            </Typography>
            <Typography color="text.secondary" variant="caption" sx={{ display: "block", lineHeight: 1.3 }}>
              {lead.industry || lead.ownerType}
            </Typography>
          </Box>
          <Stack alignItems="flex-end" spacing={0.5} flexShrink={0}>
            <Chip label={getTierLabelForLead(lead)} size="small" sx={{ ...compactChipSx, bgcolor: tierStyle.text, color: "#fff", fontWeight: 700 }} />
            <Chip
              color={getActionTone(lead.action)}
              label={lead.action}
              size="small"
              variant="outlined"
              sx={{ ...compactChipSx, fontSize: "0.75rem" }}
            />
          </Stack>
        </Stack>

        {/* Score bar - single row with flex wrap */}
        <Stack direction="row" flexWrap="wrap" gap={0.6} sx={{ mt: 1 }}>
          <Chip label={`Eye ${lead.eyeLeadScore ?? 0}`} size="small" sx={compactChipSx} />
          <Chip label={`PPE ${lead.ppeScore ?? 0}`} size="small" sx={compactChipSx} />
          <Chip label={`Total ${lead.finalScore ?? 0}`} size="small" variant="outlined" sx={compactChipSx} />
          <Chip label={lead.employeeBand} size="small" variant="outlined" sx={compactChipSx} />
          {lead.totalCurrentPenalty > 0 ? (
            <Chip
              label={`$${lead.totalCurrentPenalty.toLocaleString()}`}
              size="small"
              color="error"
              variant="outlined"
              sx={compactChipSx}
            />
          ) : null}
          {lead.openViolations ? (
            <Chip color="error" label="Open" size="small" sx={compactChipSx} />
          ) : null}
          {lead.willfulViolationCount > 0 ? (
            <Chip color="error" label="Willful" size="small" sx={compactChipSx} />
          ) : null}
          {lead.repeatViolationCount > 0 ? (
            <Chip color="warning" label="Repeat" size="small" sx={compactChipSx} />
          ) : null}
        </Stack>
      </Box>

      <CardContent sx={{ pt: 1.25, pb: 1.25, px: pad }}>
        {/* ── PITCH ── */}
        {lead.pitchRecommendation && (
          <Typography variant="body2" sx={{ fontStyle: "italic", color: "text.secondary", mb: 1 }}>
            {lead.pitchRecommendation}
          </Typography>
        )}

        {/* ── EYE INJURIES ── */}
        {lead.eyeInjuryCount > 0 && (
          <Box
            sx={{
              bgcolor: "#fef2f2",
              border: "1px solid #fca5a5",
              borderRadius: 1.5,
              px: 1.25,
              py: 0.875,
              mb: 1,
            }}
          >
            <Typography variant="subtitle2" sx={{ fontSize: "0.85rem", fontWeight: 600, color: "error.dark", mb: 0.5 }}>
              Eye Injuries ({lead.eyeInjuryCount}{lead.fatalityCount > 0 ? ` +${lead.fatalityCount} fatal` : ""})
            </Typography>
            {lead.eyeInjuryDescriptions && lead.eyeInjuryDescriptions.length > 0 && (
              <Stack spacing={0.25}>
                {lead.eyeInjuryDescriptions.slice(0, 2).map((desc, i) => (
                  <Typography key={i} variant="caption" color="error.dark" sx={{ lineHeight: 1.3 }}>
                    {desc}
                  </Typography>
                ))}
              </Stack>
            )}
            {lead.lastEyeInjuryDate && (
              <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 0.5 }}>
                {lead.lastEyeInjuryDate}
              </Typography>
            )}
          </Box>
        )}

        {/* ── EYE/FACE VIOLATIONS ── */}
        {lead.eyeViolationCount > 0 && (
          <Box
            sx={{
              bgcolor: "#fff7ed",
              border: "1px solid #fdba74",
              borderRadius: 1.5,
              px: 1.25,
              py: 0.875,
              mb: 1,
            }}
          >
            <Typography variant="subtitle2" sx={{ fontSize: "0.85rem", fontWeight: 600, color: "warning.dark", mb: 0.5 }}>
              Eye/Face Citations ({lead.eyeViolationCount}
              {lead.openEyeViolationCount > 0 ? `, ${lead.openEyeViolationCount} open` : ""})
              {lead.prescriptionViolationCount > 0 ? " *Rx" : ""}
            </Typography>
            <Stack spacing={0.5}>
              {normalizedViolations
                .filter((v) => v.code.startsWith("1910.133") || v.code.startsWith("1926.102"))
                .slice(0, 1)
                .map((v) => (
                  <Box key={v.code}>
                    <Typography variant="caption" sx={{ fontWeight: 600 }}>
                      {v.code}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ display: "block", lineHeight: 1.2 }}>
                      {v.plainEnglish}
                    </Typography>
                  </Box>
                ))}
            </Stack>
          </Box>
        )}

        {/* ── GENERAL PPE VIOLATIONS ── */}
        {lead.generalPpeViolationCount > 0 && (
          <Box
            sx={{
              bgcolor: "#fefce8",
              border: "1px solid #fde047",
              borderRadius: 1.5,
              px: 1.25,
              py: 0.875,
              mb: 1,
            }}
          >
            <Typography variant="subtitle2" sx={{ fontSize: "0.85rem", fontWeight: 600, color: "#713f12", mb: 0.5 }}>
              General PPE ({lead.generalPpeViolationCount}
              {lead.openGeneralPpeViolationCount > 0 ? `, ${lead.openGeneralPpeViolationCount} open` : ""})
            </Typography>
            <Stack spacing={0.5}>
              {normalizedViolations
                .filter((v) => v.code.startsWith("1910.132") || v.code.startsWith("1926.95"))
                .slice(0, 1)
                .map((v) => (
                  <Box key={v.code}>
                    <Typography variant="caption" sx={{ fontWeight: 600 }}>
                      {v.code}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ display: "block", lineHeight: 1.2 }}>
                      {v.plainEnglish}
                    </Typography>
                  </Box>
                ))}
            </Stack>
          </Box>
        )}

        {/* ── EMPHASIS PROGRAMS ── */}
        {lead.emphasisCodes && lead.emphasisCodes.length > 0 && (
          <Box sx={{ mb: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600, display: "block", mb: 0.5 }}>
              Emphasis {lead.eyeEmphasisCount > 0 ? "(Eye/Face)" : ""}
            </Typography>
            <Stack direction="row" flexWrap="wrap" gap={0.5}>
              {lead.emphasisCodes.slice(0, 3).map((code) => (
                <Chip key={code} label={code} size="small" color="info" variant="outlined" sx={{ height: 22, fontSize: "0.7rem" }} />
              ))}
            </Stack>
          </Box>
        )}

        {/* ── QUICK SIGNALS ── */}
        <Stack direction="row" flexWrap="wrap" gap={0.5}>
          {lead.relatedInspectionCount > 0 && (
            <Chip label={`+${lead.relatedInspectionCount} follow-up`} size="small" color="warning" variant="outlined" sx={{ height: 22, fontSize: "0.7rem" }} />
          )}
          {lead.totalInspectionCount > 1 && (
            <Chip label={`${lead.totalInspectionCount} inspections`} size="small" variant="outlined" sx={{ height: 22, fontSize: "0.7rem" }} />
          )}
          {lead.contestedViolationCount > 0 && (
            <Chip label="Contested" size="small" variant="outlined" sx={{ height: 22, fontSize: "0.7rem" }} />
          )}
        </Stack>

        {/* ── DATE LINE ── */}
        <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 0.75, lineHeight: 1.2 }}>
          {lead.lastEyeInjuryDate
            ? `Eye injury: ${lead.lastEyeInjuryDate}`
            : lead.incidentDate
            ? `${getIncidentDateLabel(lead.incidentDateSource)}: ${lead.incidentDate}`
            : "No OSHA incident/violation date on record"}
          {lead.faceHeadInjuryCount > 0 ? ` • ${lead.faceHeadInjuryCount} face/head` : ""}
        </Typography>
      </CardContent>
    </Card>
  );
}

const MemoLeadCard = React.memo(LeadCard);

type OutreachStatus = "new" | "attempted" | "connected" | "won" | "lost";
const outreachOptions: Array<{ value: OutreachStatus; label: string }> = [
  { value: "new", label: "New" },
  { value: "attempted", label: "Attempted To Reach" },
  { value: "connected", label: "Connected" },
  { value: "won", label: "Won" },
  { value: "lost", label: "Lost" },
];

function OutreachCard({
  lead,
  onSave,
}: {
  lead: LeadRecord;
  onSave: (leadId: string, outreachStatus: OutreachStatus, outreachNotes: string) => Promise<void>;
}) {
  const [outreachStatus, setOutreachStatus] = React.useState<OutreachStatus>(
    (lead.outreachStatus as OutreachStatus) || "new",
  );
  const [notes, setNotes] = React.useState(lead.outreachNotes || "");
  const [saving, setSaving] = React.useState(false);

  React.useEffect(() => {
    setOutreachStatus((lead.outreachStatus as OutreachStatus) || "new");
    setNotes(lead.outreachNotes || "");
  }, [lead.id, lead.outreachStatus, lead.outreachNotes]);

  return (
    <Card sx={{ mt: 1.25 }}>
      <CardContent sx={{ pt: 2 }}>
        <Typography variant="subtitle2">Outreach Tracking</Typography>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
          <FormControl size="small" sx={{ minWidth: 180 }}>
            <InputLabel>Status</InputLabel>
            <Select
              label="Status"
              value={outreachStatus}
              onChange={(event) => setOutreachStatus(event.target.value as OutreachStatus)}
            >
              {outreachOptions.map((option) => (
                <MenuItem key={option.value} value={option.value}>
                  {option.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth
            size="small"
            label="Notes"
            value={notes}
            onChange={(event) => setNotes(event.target.value)}
            placeholder="Left voicemail, gatekeeper response, meeting set, loss reason..."
          />
          <Button
            variant="contained"
            disabled={saving}
            sx={{ width: { xs: "100%", sm: "auto" }, minWidth: { sm: 92 } }}
            onClick={async () => {
              setSaving(true);
              try {
                await onSave(lead.id, outreachStatus, notes);
              } finally {
                setSaving(false);
              }
            }}
          >
            {saving ? "Saving..." : "Save"}
          </Button>
        </Stack>
        <Typography sx={{ mt: 1 }} color="text.secondary" variant="caption">
          Last update: {lead.outreachUpdatedAt ? formatPullTime(lead.outreachUpdatedAt) : "N/A"}
        </Typography>
      </CardContent>
    </Card>
  );
}

const MemoOutreachCard = React.memo(OutreachCard);

type PaginationControlsProps = {
  page: number;
  totalItems: number;
  pageSize: number;
  onPageChange: (page: number) => void;
};

function PaginationControls({ page, totalItems, pageSize, onPageChange }: PaginationControlsProps) {
  const totalPages = Math.ceil(totalItems / pageSize);
  if (totalPages <= 1) return null;
  const start = page * pageSize + 1;
  const end = Math.min((page + 1) * pageSize, totalItems);
  return (
    <Stack direction="row" alignItems="center" justifyContent="center" spacing={1} sx={{ pt: 0.5 }}>
      <Button size="small" variant="outlined" disabled={page === 0} onClick={() => onPageChange(0)}>«</Button>
      <Button size="small" variant="outlined" disabled={page === 0} onClick={() => onPageChange(page - 1)}>‹ Prev</Button>
      <Typography variant="body2" color="text.secondary" sx={{ px: 1 }}>
        {start}–{end} of {totalItems}
      </Typography>
      <Button size="small" variant="outlined" disabled={page >= totalPages - 1} onClick={() => onPageChange(page + 1)}>Next ›</Button>
      <Button size="small" variant="outlined" disabled={page >= totalPages - 1} onClick={() => onPageChange(totalPages - 1)}>»</Button>
    </Stack>
  );
}

type AccountStatusBadgeProps = { status: AccountListStatus; label?: string };
function AccountStatusBadge({ status, label }: AccountStatusBadgeProps) {
  const icons: Record<AccountListStatus, React.ReactNode> = {
    cold: "❄️",
    warm: "🔆",
    hot: "🔥",
    meeting: "📅",
    not_interested: "🚫",
    unqualified: "🚫",
  };
  return (
    <Chip
      size="small"
      color={ACCOUNT_STATUS_COLORS[status]}
      variant={status === "not_interested" || status === "unqualified" ? "filled" : "outlined"}
      label={`${icons[status]} ${label ?? ACCOUNT_STATUS_LABELS[status]}`}
      sx={{ fontWeight: 600, fontSize: "0.7rem", alignSelf: "flex-start" }}
    />
  );
}

function formatPullTime(isoTime?: string | null) {
  if (!isoTime) return "N/A";
  const date = new Date(isoTime);
  if (Number.isNaN(date.getTime())) return isoTime;
  return date.toLocaleString();
}

function getIncidentDateLabel(source?: IncidentDateSource) {
  switch (source) {
    case "accident":
      return "Accident date";
    case "violation-event":
      return "Violation event date";
    case "case-close":
      return "Case close date";
    case "case-open":
      return "Case open date";
    default:
      return "Incident date";
  }
}

function isTrueIncidentSource(source?: IncidentDateSource) {
  return source === "accident" || source === "violation-event";
}

export default function App() {
  const [mobileOpen, setMobileOpen] = React.useState(false);
  const [desktopNavCollapsed, setDesktopNavCollapsed] = React.useState(false);
  const [showAdvancedFilters, setShowAdvancedFilters] = React.useState(false);
  const [activeView, setActiveView] = React.useState<NavView>("overview");
  const [query, setQuery] = React.useState("");
  const [regionFilter, setRegionFilter] = React.useState("All");
  const [countyFilter, setCountyFilter] = React.useState("All");
  const [priorityFilter, setPriorityFilter] = React.useState("All");
  const [sourceFilter, setSourceFilter] = React.useState("All");
  const [incidentFilter, setIncidentFilter] = React.useState("All");
  const [leadTypeFilter, setLeadTypeFilter] = React.useState("All");
  const [geoMatchFilter, setGeoMatchFilter] = React.useState("All");
  const [industryFilter, setIndustryFilter] = React.useState("All");
  const [settings, setSettings] = React.useState(initialSettings);
  const [liveLeads, setLiveLeads] = React.useState<LeadRecord[]>([]);
  const [totalAvailableLeads, setTotalAvailableLeads] = React.useState<number | null>(null);
  const [loadingLeads, setLoadingLeads] = React.useState(true);
  const [leadLoadError, setLeadLoadError] = React.useState<string | null>(null);
  const [pullStatus, setPullStatus] = React.useState<PullStatus | null>(null);
  const [pullHistory, setPullHistory] = React.useState<PullHistoryItem[]>([]);
  const [triggeringPull, setTriggeringPull] = React.useState(false);
  const [triggeringFullPull, setTriggeringFullPull] = React.useState(false);
  const [reloadingBigQuery, setReloadingBigQuery] = React.useState(false);
  const [pageSize, setPageSize] = React.useState(DEFAULT_PAGE_SIZE);
  const [pageByView, setPageByView] = React.useState<Record<string, number>>({
    "lead-queue": 0,
    "hot-eye-leads": 0,
    "ppe-opportunity": 0,
  });
  const desktopDrawerWidth = desktopNavCollapsed ? drawerWidthCollapsed : drawerWidthExpanded;

  // Bad lead state — persisted to localStorage, filtered out of all views
  const [badLeads, setBadLeads] = React.useState<BadLeadEntry[]>(() => loadBadLeads());
  const badLeadIds = React.useMemo(() => new Set(badLeads.map((b) => b.leadId)), [badLeads]);
  const [badLeadDialogLead, setBadLeadDialogLead] = React.useState<LeadRecord | null>(null);

  // Named / known accounts — prospect list pre-seeded from sales team
  const [namedAccounts, setNamedAccounts] = React.useState<Record<string, NamedAccountEntry>>(() => loadNamedAccounts());

  const dncKeys = React.useMemo(
    () =>
      new Set(
        Object.entries(namedAccounts)
          .filter(([, e]) => e.status === "not_interested" || e.status === "unqualified")
          .map(([k]) => k),
      ),
    [namedAccounts],
  );
  const [badLeadReason, setBadLeadReason] = React.useState<BadLeadReason>("wrong_industry");

  // NAICS suppression rules — auto-filter companies in the same subsector as dismissed leads
  const [naicsRules, setNaicsRules] = React.useState<NaicsRule[]>(() => loadNaicsRules());

  const loadLeads = React.useCallback(async (force = false) => {
    setLoadingLeads(true);
    setLeadLoadError(null);
    try {
      const response = await fetch(force ? "/api/leads?force=1" : "/api/leads");
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Failed to load leads");
      }
      // Merge locally-persisted outcomes so saves survive Vercel cold starts
      const withLocal = applyLocalOutcomes(payload.leads || [], loadOutcomes());
      setLiveLeads(withLocal);
      setTotalAvailableLeads(Number.isFinite(Number(payload.totalAvailable)) ? Number(payload.totalAvailable) : null);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load leads";
      setLeadLoadError(message);
      setLiveLeads([]);
      setTotalAvailableLeads(null);
    } finally {
      setLoadingLeads(false);
    }
  }, []);

  const loadPullHistory = React.useCallback(async () => {
    try {
      const response = await fetch("/api/pull-history");
      const payload = await response.json();
      if (response.ok && payload.ok) {
        setPullHistory(payload.history || []);
      }
    } catch {
      setPullHistory([]);
    }
  }, []);

  const loadPullStatus = React.useCallback(async () => {
    try {
      const response = await fetch("/api/pull-status");
      const payload = await response.json();
      if (response.ok && payload.ok) {
        setPullStatus(payload.currentPull || null);
      }
    } catch {
      setPullStatus(null);
    }
  }, []);

  React.useEffect(() => {
    loadLeads();
    loadPullHistory();
    loadPullStatus();
  }, [loadLeads, loadPullHistory, loadPullStatus]);

  React.useEffect(() => {
    if (pullStatus?.status !== "running") {
      return undefined;
    }

    const timer = setInterval(async () => {
      await loadPullStatus();
      await loadPullHistory();
    }, 4000);

    return () => clearInterval(timer);
  }, [pullStatus, loadPullStatus, loadPullHistory]);

  React.useEffect(() => {
    if (pullStatus && pullStatus.status !== "running") {
      loadLeads(false);
    }
  }, [pullStatus, loadLeads]);

  const onTriggerPull = async () => {
    setTriggeringPull(true);
    try {
      const response = await fetch("/api/refresh-leads", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Could not start pull");
      }
      setPullStatus(payload.pull);
      await loadPullHistory();
    } catch (error) {
      setLeadLoadError(error instanceof Error ? error.message : "Failed to trigger pull");
    } finally {
      setTriggeringPull(false);
    }
  };

  const onTriggerFullPull = async () => {
    setTriggeringFullPull(true);
    try {
      const response = await fetch("/api/full-pipeline", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Could not start full pull");
      }
      setPullStatus(payload.pull);
      await loadPullHistory();
    } catch (error) {
      setLeadLoadError(error instanceof Error ? error.message : "Failed to trigger full pull");
    } finally {
      setTriggeringFullPull(false);
    }
  };

  const onReloadBigQuery = async () => {
    setReloadingBigQuery(true);
    try {
      await loadLeads(true);
      await loadPullStatus();
    } finally {
      setReloadingBigQuery(false);
    }
  };

  const onSaveLeadOutcome = async (leadId: string, outreachStatus: OutreachStatus, outreachNotes: string) => {
    const accountStatus: OutcomeEntry["accountStatus"] =
      outreachStatus === "won" ? "Contacted"
      : outreachStatus === "lost" || outreachStatus === "connected" ? "In Review"
      : outreachStatus === "attempted" ? "Contacted"
      : "New";
    const outreachUpdatedAt = new Date().toISOString();

    // Persist locally first — survives Vercel cold starts and network failures
    const outcomes = loadOutcomes();
    outcomes[leadId] = { outreachStatus, outreachNotes, outreachUpdatedAt, accountStatus };
    saveOutcomes(outcomes);

    // Optimistically update UI
    setLiveLeads((current) =>
      current.map((lead) =>
        lead.id === leadId
          ? { ...lead, outreachStatus, outreachNotes, outreachUpdatedAt, accountStatus }
          : lead,
      ),
    );

    // Fire-and-forget to API (best-effort server-side logging)
    fetch("/api/lead-outcomes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ leadId, outreachStatus, outreachNotes }),
    }).catch(() => {});
  };

  const onConfirmBadLead = (lead: LeadRecord, reason: BadLeadReason) => {
    const entry: BadLeadEntry = {
      leadId: lead.id,
      company: lead.company,
      industry: lead.industry,
      city: lead.city,
      region: lead.region,
      naicsCode: lead.naicsCode,
      leadTier: lead.leadTier ?? "P3 Industry Fit",
      reason,
      markedAt: new Date().toISOString(),
    };
    const updatedBadLeads = [entry, ...badLeads.filter((b) => b.leadId !== lead.id)];
    setBadLeads(updatedBadLeads);
    saveBadLeads(updatedBadLeads);

    // For industry/consumer-type dismissals, create a NAICS suppression rule
    // that auto-hides all companies in the same subsector (4-digit prefix).
    if (BROAD_SUPPRESS_REASONS.includes(reason) && lead.naicsCode && lead.naicsCode.length >= 4) {
      const prefix = lead.naicsCode.slice(0, 4);
      const alreadyExists = naicsRules.some((r) => r.prefix === prefix);
      if (!alreadyExists) {
        const newRule: NaicsRule = {
          prefix,
          label: lead.industry,
          reason,
          exampleCompany: lead.company,
          addedAt: new Date().toISOString(),
        };
        const updatedRules = [newRule, ...naicsRules];
        setNaicsRules(updatedRules);
        saveNaicsRules(updatedRules);
      }
    }

    setBadLeadDialogLead(null);
    // Fire-and-forget to API for server-side logging
    fetch("/api/bad-leads", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(entry),
    }).catch(() => {});
  };

  const onUnmarkBadLead = (leadId: string) => {
    const updated = badLeads.filter((b) => b.leadId !== leadId);
    setBadLeads(updated);
    saveBadLeads(updated);
  };

  const onRemoveNaicsRule = (prefix: string) => {
    const updated = naicsRules.filter((r) => r.prefix !== prefix);
    setNaicsRules(updated);
    saveNaicsRules(updated);
  };

  const onRemoveNamedAccount = (key: string) => {
    const updated = { ...namedAccounts };
    delete updated[key];
    setNamedAccounts(updated);
    saveNamedAccounts(updated);
  };

  const leadData = React.useMemo(() => {
    const allLeads = liveLeads.length > 0 ? liveLeads : fallbackLeads;
    return allLeads.filter((l) => {
      if (badLeadIds.has(l.id)) return false;
      if (naicsRules.length > 0 && l.naicsCode) {
        for (const rule of naicsRules) {
          if (l.naicsCode.startsWith(rule.prefix)) return false;
        }
      }
      if (dncKeys.size > 0 && dncKeys.has(normalizeCompanyKey(l.company))) return false;
      return true;
    });
  }, [liveLeads, badLeadIds, naicsRules, dncKeys]);

  const autoSuppressedCount = React.useMemo(() => {
    const allLeads = liveLeads.length > 0 ? liveLeads : fallbackLeads;
    return allLeads.filter((l) => {
      if (badLeadIds.has(l.id)) return false; // already counted as manual dismiss
      if (naicsRules.length > 0 && l.naicsCode) {
        for (const rule of naicsRules) {
          if (l.naicsCode.startsWith(rule.prefix)) return true;
        }
      }
      return false;
    }).length;
  }, [liveLeads, badLeadIds, naicsRules]);

  const regionOptions = React.useMemo(
    () => Array.from(new Set(leadData.map((lead) => lead.region).filter(Boolean))).sort(),
    [leadData],
  );

  const industryOptions = React.useMemo(
    () => Array.from(new Set(leadData.map((lead) => lead.industry).filter(Boolean))).sort(),
    [leadData],
  );

  const countyOptions = React.useMemo(
    () =>
      Array.from(
        new Set(
          leadData
            .filter((lead) => regionFilter === "All" || lead.region === regionFilter)
            .map((lead) => lead.county || "")
            .filter(Boolean),
        ),
      ).sort(),
    [leadData, regionFilter],
  );

  const sourceOptions = React.useMemo(
    () =>
      Array.from(
        new Set(
          leadData
            .flatMap((lead) => lead.matchedSources || [])
            .filter(Boolean),
        ),
      ).sort(),
    [leadData],
  );

  React.useEffect(() => {
    if (countyFilter !== "All" && !countyOptions.includes(countyFilter)) {
      setCountyFilter("All");
    }
  }, [countyFilter, countyOptions]);

  React.useEffect(() => {
    if (sourceFilter !== "All" && !sourceOptions.includes(sourceFilter)) {
      setSourceFilter("All");
    }
  }, [sourceFilter, sourceOptions]);

  const visibleLeads = React.useMemo(
    () =>
      leadData.filter((lead) => {
        if (!matchesSearch(lead, query)) return false;
        if (regionFilter !== "All" && lead.region !== regionFilter) return false;
        if (countyFilter !== "All" && (lead.county || "") !== countyFilter) return false;
        if (priorityFilter !== "All" && lead.leadTier !== priorityFilter) return false;
        if (sourceFilter !== "All" && !lead.matchedSources.includes(sourceFilter)) return false;
        if (incidentFilter !== "All" && lead.incidentType !== incidentFilter) return false;
        if (leadTypeFilter !== "All" && (lead.leadType || "profile_fit") !== leadTypeFilter) return false;
        if (geoMatchFilter !== "All" && (lead.geoMatchSource || "none") !== geoMatchFilter) return false;
        if (industryFilter !== "All" && lead.industry !== industryFilter) return false;
        if (settings.showOnlyContactReady && !CONTACT_READY_ACTIONS.includes(lead.action)) {
          return false;
        }
        return true;
      }),
    [
      leadData,
      query,
      regionFilter,
      countyFilter,
      priorityFilter,
      sourceFilter,
      incidentFilter,
      leadTypeFilter,
      geoMatchFilter,
      industryFilter,
      settings.showOnlyContactReady,
    ],
  );

  const contactReadyLoadedCount = React.useMemo(
    () => leadData.filter((lead) => CONTACT_READY_ACTIONS.includes(lead.action)).length,
    [leadData],
  );

  const hasActiveFilters = React.useMemo(
    () =>
      regionFilter !== "All" ||
      countyFilter !== "All" ||
      priorityFilter !== "All" ||
      sourceFilter !== "All" ||
      incidentFilter !== "All" ||
      leadTypeFilter !== "All" ||
      geoMatchFilter !== "All" ||
      industryFilter !== "All" ||
      query.trim().length > 0 ||
      settings.showOnlyContactReady,
    [
      regionFilter,
      countyFilter,
      priorityFilter,
      sourceFilter,
      incidentFilter,
      leadTypeFilter,
      geoMatchFilter,
      industryFilter,
      query,
      settings.showOnlyContactReady,
    ],
  );

  const clearAllFilters = () => {
    setRegionFilter("All");
    setCountyFilter("All");
    setPriorityFilter("All");
    setSourceFilter("All");
    setIncidentFilter("All");
    setLeadTypeFilter("All");
    setGeoMatchFilter("All");
    setIndustryFilter("All");
    setQuery("");
    setSettings((current) => ({ ...current, showOnlyContactReady: false }));
  };

  const hotEyeLeads = React.useMemo(
    () =>
      visibleLeads.filter(
        (lead) => lead.leadTier === "P0 Hot Eye" || lead.leadTier === "P1 Eye Violation",
      ),
    [visibleLeads],
  );
  const hotAccounts = hotEyeLeads; // legacy compat for overview section
  const monitorLeads = React.useMemo(
    () => visibleLeads.filter((lead) => lead.leadTier === "P3 Industry Fit"),
    [visibleLeads],
  );
  const contactReadyLeads = React.useMemo(
    () => visibleLeads.filter((lead) => CONTACT_READY_ACTIONS.includes(lead.action)),
    [visibleLeads],
  );
  const ppeOpportunityLeads = React.useMemo(
    () => visibleLeads.filter((lead) => lead.leadTier === "P2 PPE Opportunity"),
    [visibleLeads],
  );
  const leadQueueLeads = React.useMemo(() => {
    if (settings.showOnlyContactReady) {
      return contactReadyLeads;
    }

    // Keep queue actionable first, then append a smaller monitor watchlist.
    const monitorWatchlist = monitorLeads.slice(0, 120);
    return [...contactReadyLeads, ...monitorWatchlist];
  }, [contactReadyLeads, monitorLeads, settings.showOnlyContactReady]);
  const researchNeeded = ppeOpportunityLeads; // legacy compat
  const sourceSignalRows = React.useMemo(
    () =>
      visibleLeads.flatMap((lead) =>
        lead.matchedSources.map((source) => ({
          id: `${lead.id}-${source}`,
          company: lead.company,
          source,
          region: lead.region,
          incidentDate: lead.incidentDate,
          incidentType: lead.incidentType,
          codes: lead.rawViolationCodes.join(", "),
          plainEnglish: toViolationDetails(lead.rawViolationCodes)
            .map((item) => `${item.code}: ${item.plainEnglish}`)
            .join(" | "),
          score: lead.overallSalesScore,
          note: lead.reasonToContact,
        })),
      ),
    [visibleLeads],
  );

  const recentIncidents = React.useMemo(
    () =>
      visibleLeads.filter((lead) => {
        const dateStr = lead.lastEyeInjuryDate || (isTrueIncidentSource(lead.incidentDateSource) ? lead.incidentDate : null);
        if (!dateStr) return false;
        const daysSinceIncident = Math.floor(
          (Date.now() - new Date(`${dateStr}T00:00:00`).getTime()) / (1000 * 60 * 60 * 24),
        );
        return daysSinceIncident <= 30;
      }),
    [visibleLeads],
  );

  // Reset to page 0 when filters or page size change
  React.useEffect(() => {
    setPageByView({ "lead-queue": 0, "hot-eye-leads": 0, "ppe-opportunity": 0 });
  }, [visibleLeads.length, pageSize]);

  const leadQueueVisibleRows = React.useMemo(() => {
    const p = pageByView["lead-queue"] ?? 0;
    return leadQueueLeads.slice(p * pageSize, (p + 1) * pageSize);
  }, [leadQueueLeads, pageByView, pageSize]);

  const hotEyeVisibleRows = React.useMemo(() => {
    const p = pageByView["hot-eye-leads"] ?? 0;
    return hotEyeLeads.slice(p * pageSize, (p + 1) * pageSize);
  }, [hotEyeLeads, pageByView, pageSize]);

  const ppeVisibleRows = React.useMemo(() => {
    const p = pageByView["ppe-opportunity"] ?? 0;
    return ppeOpportunityLeads.slice(p * pageSize, (p + 1) * pageSize);
  }, [ppeOpportunityLeads, pageByView, pageSize]);

  const goToPage = (view: "lead-queue" | "hot-eye-leads" | "ppe-opportunity", page: number) => {
    setPageByView((current) => ({ ...current, [view]: page }));
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const navCounts: Record<NavView, number | string> = {
    overview: "",
    "lead-queue": leadQueueLeads.length,
    "hot-eye-leads": hotEyeLeads.length,
    "ppe-opportunity": ppeOpportunityLeads.length,
    "source-signals": visibleLeads.length,
    settings: badLeads.length + naicsRules.length > 0 ? badLeads.length + naicsRules.length : "",
  };

  const latestPull = pullHistory[0] || null;
  const attemptedCount = visibleLeads.filter((lead) => lead.outreachStatus === "attempted").length;
  const connectedCount = visibleLeads.filter((lead) => lead.outreachStatus === "connected").length;
  const wonCount = visibleLeads.filter((lead) => lead.outreachStatus === "won").length;
  const lostCount = visibleLeads.filter((lead) => lead.outreachStatus === "lost").length;

  const renderLeadWorkflowCard = (lead: LeadRecord) => {
    const accountEntry = namedAccounts[normalizeCompanyKey(lead.company)];
    return (
      <Stack
        spacing={1}
        sx={{
          width: "100%",
          borderRadius: 3,
          border: "1px solid rgba(15, 23, 42, 0.1)",
          p: 1.25,
          bgcolor: alpha("#ffffff", 0.74),
          backdropFilter: "blur(4px)",
        }}
      >
        <MemoLeadCard lead={lead} compact={settings.compactCards} />
        {accountEntry ? (
          <Box>
            <AccountStatusBadge status={accountEntry.status} label={ACCOUNT_STATUS_LABELS[accountEntry.status]} />
          </Box>
        ) : null}
        <Divider sx={{ borderColor: "rgba(15, 23, 42, 0.08)" }} />
        <MemoOutreachCard lead={lead} onSave={onSaveLeadOutcome} />
        <Box>
          <Button
            size="small"
            variant="outlined"
            color="error"
            startIcon={<BlockRoundedIcon />}
            onClick={() => {
              setBadLeadReason("wrong_industry");
              setBadLeadDialogLead(lead);
            }}
            sx={{ alignSelf: "flex-start", opacity: 0.86, "&:hover": { opacity: 1 } }}
          >
            Not a Fit
          </Button>
        </Box>
      </Stack>
    );
  };

  const renderDrawer = (collapsed: boolean) => (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Box sx={{ px: collapsed ? 1.5 : 3, pt: 3, pb: 2 }}>
        <Stack direction="row" spacing={1.5} alignItems="center" justifyContent={collapsed ? "center" : "flex-start"}>
          <Avatar sx={{ bgcolor: "#c7773f", color: "#fff" }}>
            <AutoAwesomeRoundedIcon />
          </Avatar>
          {!collapsed ? (
            <Box>
              <Typography variant="h6">Lead Signal Desk</Typography>
              <Typography sx={{ opacity: 0.78 }} variant="body2">
                Daily sales pipeline navigator
              </Typography>
            </Box>
          ) : null}
        </Stack>
      </Box>
      <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />
      <List sx={{ px: collapsed ? 1 : 1.5, py: 2 }}>
        {navItems.map((item) => (
          <ListItemButton
            key={item.view}
            onClick={() => {
              setActiveView(item.view);
              setMobileOpen(false);
            }}
            selected={activeView === item.view}
            sx={{
              borderRadius: 3,
              mb: 0.5,
              color: "#f7f2e8",
              justifyContent: collapsed ? "center" : "flex-start",
              px: collapsed ? 1 : 1.5,
              "&.Mui-selected": {
                backgroundColor: "rgba(255,255,255,0.14)",
              },
            }}
          >
            <ListItemIcon sx={{ color: "inherit", minWidth: collapsed ? 0 : 40 }}>{item.icon}</ListItemIcon>
            {!collapsed ? <ListItemText primary={item.label} /> : null}
            {!collapsed && navCounts[item.view] !== "" ? <span className="nav-pill">{navCounts[item.view]}</span> : null}
          </ListItemButton>
        ))}
      </List>

      {!collapsed ? (
        <Box sx={{ mt: "auto", p: 2 }}>
          <Card
            sx={{
              bgcolor: alpha("#ffffff", 0.08),
              color: "#fff9f1",
              border: "1px solid rgba(255,255,255,0.08)",
              boxShadow: "none",
            }}
          >
            <CardContent>
              <Typography variant="overline">Today's focus</Typography>
              <Typography sx={{ mt: 1 }} variant="h6">
                {hotAccounts.length} accounts are ready for immediate contact
              </Typography>
              <Typography sx={{ mt: 1, opacity: 0.8 }} variant="body2">
                Pull status: {pullStatus?.status || "idle"}.
              </Typography>
            </CardContent>
          </Card>
        </Box>
      ) : null}
    </Box>
  );

  return (
    <Box sx={{ display: "flex", minHeight: "100vh" }}>
      <AppBar
        color="inherit"
        elevation={0}
        position="fixed"
        sx={{
          width: { md: `calc(100% - ${desktopDrawerWidth}px)` },
          ml: { md: `${desktopDrawerWidth}px` },
          borderBottom: "1px solid rgba(31, 41, 55, 0.08)",
          backdropFilter: "blur(18px)",
          backgroundColor: alpha("#f4efe7", 0.82),
        }}
      >
        <Toolbar sx={{ gap: { xs: 1, md: 1.5 }, minHeight: { xs: 64, sm: 72, md: 64 } }}>
          <IconButton onClick={() => setMobileOpen(true)} sx={{ display: { md: "none" } }}>
            <MenuRoundedIcon />
          </IconButton>
          <Tooltip title={desktopNavCollapsed ? "Expand sidebar" : "Collapse sidebar"}>
            <IconButton
              onClick={() => setDesktopNavCollapsed((current) => !current)}
              sx={{ display: { xs: "none", md: "inline-flex" } }}
            >
              {desktopNavCollapsed ? <ChevronRightRoundedIcon /> : <ChevronLeftRoundedIcon />}
            </IconButton>
          </Tooltip>
          <TextField
            sx={{ flex: 1, minWidth: { xs: 140, sm: 260 } }}
            placeholder="Search company, code, incident type, source signal, or layman summary..."
            size="small"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchRoundedIcon color="action" />
                </InputAdornment>
              ),
            }}
          />

          <Stack
            direction="row"
            spacing={0.75}
            sx={{
              flexShrink: 0,
              overflowX: { xs: "auto", md: "visible" },
              maxWidth: { xs: "58vw", md: "none" },
              scrollbarWidth: "none",
              "&::-webkit-scrollbar": { display: "none" },
              "& .toolbar-action": {
                borderRadius: 2,
                textTransform: "none",
                whiteSpace: "nowrap",
                height: { xs: 34, sm: 36 },
                minWidth: { xs: 36, sm: 102 },
                px: { xs: 1, sm: 1.5 },
                fontSize: { xs: "0.75rem", sm: "0.8125rem" },
                "& .MuiButton-startIcon": {
                  marginLeft: 0,
                  marginRight: { xs: 0, sm: 0.75 },
                },
                "& .btn-label": {
                  display: { xs: "none", sm: "inline" },
                },
                "& .btn-label-short": {
                  display: { xs: "inline", sm: "none" },
                },
              },
            }}
          >
            <Button
              className="toolbar-action"
              color="secondary"
              disabled={triggeringPull || triggeringFullPull || pullStatus?.status === "running"}
              startIcon={
                triggeringPull || pullStatus?.status === "running" ? (
                  <CircularProgress color="inherit" size={16} />
                ) : (
                  <RefreshRoundedIcon />
                )
              }
              variant="contained"
              onClick={onTriggerPull}
            >
              <span className="btn-label">Quick Refresh</span>
              <span className="btn-label-short">Quick</span>
            </Button>
            <Button
              className="toolbar-action"
              color="warning"
              disabled={triggeringPull || triggeringFullPull || pullStatus?.status === "running"}
              startIcon={
                triggeringFullPull ? <CircularProgress color="inherit" size={16} /> : <AutoAwesomeRoundedIcon />
              }
              variant="outlined"
              onClick={onTriggerFullPull}
            >
              <span className="btn-label">Full Pull</span>
              <span className="btn-label-short">Full</span>
            </Button>
            <Button
              className="toolbar-action"
              color="info"
              disabled={loadingLeads || reloadingBigQuery || pullStatus?.status === "running"}
              startIcon={reloadingBigQuery ? <CircularProgress color="inherit" size={16} /> : <SourceRoundedIcon />}
              variant="outlined"
              onClick={onReloadBigQuery}
            >
              <span className="btn-label">Reload BigQuery</span>
              <span className="btn-label-short">Reload</span>
            </Button>
            <Button
              className="toolbar-action"
              startIcon={<TuneRoundedIcon />}
              variant="contained"
              onClick={() => setActiveView("settings")}
            >
              <span className="btn-label">Settings</span>
              <span className="btn-label-short">Settings</span>
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: desktopDrawerWidth }, flexShrink: { md: 0 } }}>
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={() => setMobileOpen(false)}
          ModalProps={{ keepMounted: true }}
          sx={{ display: { xs: "block", md: "none" }, "& .MuiDrawer-paper": { width: drawerWidthExpanded } }}
        >
          {renderDrawer(false)}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: "none", md: "block" },
            "& .MuiDrawer-paper": { width: desktopDrawerWidth, boxSizing: "border-box" },
          }}
          open
        >
          {renderDrawer(desktopNavCollapsed)}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: { md: `calc(100% - ${desktopDrawerWidth}px)` },
          px: { xs: 2, md: 4 },
          py: 4,
        }}
      >
        <Toolbar />

        <Stack spacing={3}>
          {leadLoadError ? <Alert severity="warning">Live load issue: {leadLoadError}. Showing fallback sample data.</Alert> : null}
          {loadingLeads ? <Alert severity="info">Loading live leads from BigQuery...</Alert> : null}
          {pullStatus?.status === "running" ? (
            <Alert severity="info">
              {pullStatus.mode === "full"
                ? `Full pipeline started at ${formatPullTime(pullStatus.startedAt)}. This is heavier and can take up to 15 minutes.`
                : `Quick refresh started at ${formatPullTime(pullStatus.startedAt)}. This should complete in about 1-3 minutes.`}
            </Alert>
          ) : null}
          {pullStatus?.status === "failed" ? (
            <Alert severity="error">
              Last pull failed at {formatPullTime(pullStatus.endedAt)}: {pullStatus.error || "unknown error"}
            </Alert>
          ) : null}
          {pullStatus?.status === "success" ? (
            <Alert severity="success">
              Last pull completed at {formatPullTime(pullStatus.endedAt)} ({pullStatus.durationSeconds || 0}s).
            </Alert>
          ) : null}

          <Box>
            <Typography variant="h3">
              {activeView === "overview" && "Lead Overview"}
              {activeView === "lead-queue" && "Lead Queue"}
              {activeView === "hot-eye-leads" && "Hot Eye Leads"}
              {activeView === "ppe-opportunity" && "PPE Opportunity"}
              {activeView === "source-signals" && "Source Signals"}
              {activeView === "settings" && "Settings"}
            </Typography>
            <Typography sx={{ mt: 1, maxWidth: 760 }} color="text.secondary" variant="body1">
              Keep the contact decisions obvious: who is urgent, why they matter, what evidence supports the call, and
              what follow-up path each lead needs next.
            </Typography>
            <Typography sx={{ mt: 1 }} color="text.secondary" variant="body2">
              Most recent pull: {latestPull ? `${latestPull.status} at ${formatPullTime(latestPull.endedAt || latestPull.startedAt)}` : "none yet"}
            </Typography>
          </Box>

          <Card>
            <CardContent>
              <Stack
                direction={{ xs: "column", sm: "row" }}
                justifyContent="space-between"
                alignItems={{ xs: "flex-start", sm: "center" }}
                spacing={1}
                sx={{ mb: 1.5 }}
              >
                <Stack spacing={0.4}>
                  <Typography variant="body2" color="text.secondary">
                    Showing {visibleLeads.length} of {leadData.length} leads
                    {settings.showOnlyContactReady ? " (contact-ready only)" : ""}
                  </Typography>
                  {totalAvailableLeads != null ? (
                    <Typography variant="caption" color="text.secondary">
                      Total available in BigQuery: {totalAvailableLeads}
                    </Typography>
                  ) : null}
                  <Typography variant="caption" color="text.secondary">
                    Contact-ready loaded: {contactReadyLoadedCount}
                  </Typography>
                </Stack>
                <Stack direction="row" spacing={0.75}>
                  <Button
                    size="small"
                    variant={settings.showOnlyContactReady ? "contained" : "outlined"}
                    onClick={() =>
                      setSettings((current) => ({
                        ...current,
                        showOnlyContactReady: !current.showOnlyContactReady,
                      }))
                    }
                  >
                    {settings.showOnlyContactReady ? "Show All Loaded" : "Show Contact-Ready"}
                  </Button>
                  <Button
                    size="small"
                    variant={showAdvancedFilters ? "contained" : "outlined"}
                    onClick={() => setShowAdvancedFilters((current) => !current)}
                  >
                    {showAdvancedFilters ? "Hide Advanced" : "More Filters"}
                  </Button>
                  <Button
                    size="small"
                    variant="text"
                    disabled={!hasActiveFilters}
                    onClick={clearAllFilters}
                  >
                    Clear Filters
                  </Button>
                </Stack>
              </Stack>
              <Grid container spacing={2}>
                <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Region</InputLabel>
                    <Select label="Region" value={regionFilter} onChange={(event) => setRegionFilter(event.target.value)}>
                      <MenuItem value="All">All regions</MenuItem>
                      {regionOptions.map((region) => (
                        <MenuItem key={region} value={region}>{region}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Priority</InputLabel>
                    <Select
                      label="Priority"
                      value={priorityFilter}
                      onChange={(event) => setPriorityFilter(event.target.value)}
                    >
                      <MenuItem value="All">All priorities</MenuItem>
                      <MenuItem value="P0 Hot Eye">P0 Hot Eye</MenuItem>
                      <MenuItem value="P1 Eye Violation">P1 Eye Violation</MenuItem>
                      <MenuItem value="P2 PPE Opportunity">P2 PPE Opportunity</MenuItem>
                      <MenuItem value="P3 Industry Fit">P3 Industry Fit</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Incident Type</InputLabel>
                    <Select
                      label="Incident Type"
                      value={incidentFilter}
                      onChange={(event) => setIncidentFilter(event.target.value)}
                    >
                      <MenuItem value="All">All incident types</MenuItem>
                      {incidentOptions.map((incidentType) => (
                        <MenuItem key={incidentType} value={incidentType}>
                          {incidentType}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Industry</InputLabel>
                    <Select
                      label="Industry"
                      value={industryFilter}
                      onChange={(event) => setIndustryFilter(event.target.value)}
                    >
                      <MenuItem value="All">All industries</MenuItem>
                      {industryOptions.map((ind) => (
                        <MenuItem key={ind} value={ind}>{ind}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, sm: 6, md: 4, lg: 2 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Per page</InputLabel>
                    <Select
                      label="Per page"
                      value={pageSize}
                      onChange={(event) => setPageSize(Number(event.target.value))}
                    >
                      {PAGE_SIZE_OPTIONS.map((n) => (
                        <MenuItem key={n} value={n}>{n} per page</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                {showAdvancedFilters ? (
                  <>
                    <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                      <FormControl fullWidth size="small">
                        <InputLabel>County</InputLabel>
                        <Select label="County" value={countyFilter} onChange={(event) => setCountyFilter(event.target.value)}>
                          <MenuItem value="All">All counties</MenuItem>
                          {countyOptions.map((county) => (
                            <MenuItem key={county} value={county}>{county} County</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Source</InputLabel>
                        <Select label="Source" value={sourceFilter} onChange={(event) => setSourceFilter(event.target.value)}>
                          <MenuItem value="All">All sources</MenuItem>
                          {sourceOptions.map((source) => (
                            <MenuItem key={source} value={source}>{source}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Lead Type</InputLabel>
                        <Select
                          label="Lead Type"
                          value={leadTypeFilter}
                          onChange={(event) => setLeadTypeFilter(event.target.value)}
                        >
                          <MenuItem value="All">All lead types</MenuItem>
                          <MenuItem value="incident">Incident (3-year PPE/Eye-Face)</MenuItem>
                          <MenuItem value="profile_fit">Profile Fit (No qualifying 3-year incident)</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid size={{ xs: 12, sm: 6, md: 4, lg: 3 }}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Geo Match</InputLabel>
                        <Select
                          label="Geo Match"
                          value={geoMatchFilter}
                          onChange={(event) => setGeoMatchFilter(event.target.value)}
                        >
                          <MenuItem value="All">All geo matches</MenuItem>
                          <MenuItem value="bay_radius">Bay Area within 50 miles</MenuItem>
                          <MenuItem value="san_diego_area">San Diego area</MenuItem>
                          <MenuItem value="bay_radius|san_diego_area">Bay + San Diego overlap</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                  </>
                ) : null}
              </Grid>
            </CardContent>
          </Card>

          {activeView === "overview" ? (
            <>
              <Grid container spacing={2.5}>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Hot Eye Leads"
                    value={`${hotEyeLeads.length}`}
                    supporting="P0 + P1: direct eye injury evidence or eye citation."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Eye Injury Companies"
                    value={`${visibleLeads.filter((l) => l.eyeInjuryCount > 0).length}`}
                    supporting="Companies with OSHA-recorded eye injuries in the past year."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Prescription Citations"
                    value={`${visibleLeads.filter((l) => l.prescriptionViolationCount > 0).length}`}
                    supporting="Cited for prescription lens protection failure."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Open Eye Violations"
                    value={`${visibleLeads.filter((l) => l.openEyeViolationCount > 0).length}`}
                    supporting="Eye protection violations still unabated today."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="PPE Opportunity"
                    value={`${ppeOpportunityLeads.length}`}
                    supporting="P2: general PPE leads — prescription eyewear upsell."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Recent Eye Incidents"
                    value={`${recentIncidents.length}`}
                    supporting="Eye injury / violation dates in the last 30 days."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard label="Connected" value={`${connectedCount}`} supporting="Conversations established." />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard label="Won / Lost" value={`${wonCount} / ${lostCount}`} supporting="Closed outcomes tracked." />
                </Grid>
              </Grid>

              <Grid container spacing={2.5}>
                <Grid size={{ xs: 12, lg: 7 }}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6">Best Next Calls (Hot Eye Leads)</Typography>
                      <Stack spacing={2} sx={{ mt: 2 }}>
                        {hotEyeLeads.slice(0, 4).map((lead) => (
                          <Box key={lead.id}>{renderLeadWorkflowCard(lead)}</Box>
                        ))}
                        {hotEyeLeads.length === 0 ? (
                          <Typography color="text.secondary" variant="body2">
                            No hot eye leads yet — run Quick Refresh after the next data pull.
                          </Typography>
                        ) : null}
                      </Stack>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid size={{ xs: 12, lg: 5 }}>
                  <Card sx={{ height: "100%" }}>
                    <CardContent>
                      <Typography variant="h6">Lead tier guide</Typography>
                      <Stack spacing={1.5} sx={{ mt: 2 }}>
                        <Typography variant="body2">
                          <strong>🔴 P0 Hot Eye</strong> — Direct eye injury on OSHA record. Prescription safety eyewear is urgent. Call first.
                        </Typography>
                        <Typography variant="body2">
                          <strong>🟠 P1 Eye Violation</strong> — Cited for eye/face protection failure. Compliance upgrade or prescription program opportunity. Call this week.
                        </Typography>
                        <Typography variant="body2">
                          <strong>🟡 P2 PPE Opportunity</strong> — General PPE violations in high-hazard industry. Prescription eyewear is a natural add. Warm outreach.
                        </Typography>
                        <Typography variant="body2">
                          <strong>⚪ P3 Industry Fit</strong> — Profile/industry match only. No OSHA enforcement evidence yet. Nurture or discard.
                        </Typography>
                      </Stack>
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>
            </>
          ) : null}

          {activeView === "lead-queue" ? (
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12 }}>
                <Alert severity="info">
                  Lead Queue prioritizes contact-ready accounts first, then adds a smaller P3 monitor watchlist for prospecting depth.
                </Alert>
              </Grid>
              {leadQueueVisibleRows.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <Stack spacing={1.25} sx={{ width: "100%" }}>
                    {renderLeadWorkflowCard(lead)}
                  </Stack>
                </Grid>
              ))}
              {leadQueueLeads.length > pageSize ? (
                <Grid size={{ xs: 12 }}>
                  <PaginationControls
                    page={pageByView["lead-queue"] ?? 0}
                    totalItems={leadQueueLeads.length}
                    pageSize={pageSize}
                    onPageChange={(p) => goToPage("lead-queue", p)}
                  />
                </Grid>
              ) : null}
              {leadQueueLeads.length === 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Typography color="text.secondary">No queued leads match your current filters. Click Clear Filters to widen the queue.</Typography>
                </Grid>
              ) : null}
            </Grid>
          ) : null}

          {activeView === "hot-eye-leads" ? (
            <Grid container spacing={2.5}>
              {hotEyeVisibleRows.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <Stack spacing={1.25} sx={{ width: "100%" }}>
                    {renderLeadWorkflowCard(lead)}
                  </Stack>
                </Grid>
              ))}
              {hotEyeLeads.length > pageSize ? (
                <Grid size={{ xs: 12 }}>
                  <PaginationControls
                    page={pageByView["hot-eye-leads"] ?? 0}
                    totalItems={hotEyeLeads.length}
                    pageSize={pageSize}
                    onPageChange={(p) => goToPage("hot-eye-leads", p)}
                  />
                </Grid>
              ) : null}
              {hotEyeLeads.length === 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Typography color="text.secondary">No hot eye leads match your current filters.</Typography>
                </Grid>
              ) : null}
            </Grid>
          ) : null}

          {activeView === "ppe-opportunity" ? (
            <Grid container spacing={2.5}>
              {ppeVisibleRows.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <Stack spacing={1.25} sx={{ width: "100%" }}>
                    {renderLeadWorkflowCard(lead)}
                  </Stack>
                </Grid>
              ))}
              {ppeOpportunityLeads.length > pageSize ? (
                <Grid size={{ xs: 12 }}>
                  <PaginationControls
                    page={pageByView["ppe-opportunity"] ?? 0}
                    totalItems={ppeOpportunityLeads.length}
                    pageSize={pageSize}
                    onPageChange={(p) => goToPage("ppe-opportunity", p)}
                  />
                </Grid>
              ) : null}
              {ppeOpportunityLeads.length === 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Typography color="text.secondary">No PPE opportunity leads match your current filters.</Typography>
                </Grid>
              ) : null}
            </Grid>
          ) : null}

          {activeView === "source-signals" ? (
            <Card>
              <CardContent>
                <Typography variant="h6">Source-backed lead evidence</Typography>
                <Table sx={{ mt: 2 }}>
                  <TableHead>
                    <TableRow>
                      <TableCell>Company</TableCell>
                      <TableCell>Source</TableCell>
                      <TableCell>Region</TableCell>
                      <TableCell>Incident</TableCell>
                      <TableCell>OSHA Codes</TableCell>
                      <TableCell>Score</TableCell>
                      <TableCell>Why it matters</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {sourceSignalRows.map((row) => (
                      <TableRow key={row.id}>
                        <TableCell>{row.company}</TableCell>
                        <TableCell>{row.source}</TableCell>
                        <TableCell>{row.region}</TableCell>
                        <TableCell>
                          {row.incidentDate || "N/A"}
                          <br />
                          {row.incidentType}
                        </TableCell>
                        <TableCell>
                          {row.codes || "No code"}
                          <br />
                          {row.plainEnglish || "No layman mapping available"}
                        </TableCell>
                        <TableCell>{row.score}</TableCell>
                        <TableCell>{row.note}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          ) : null}

          {activeView === "settings" ? (
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12, md: 6 }}>
                <Card>
                  <CardContent>
                    <Typography variant="h6">Display</Typography>
                    <Stack spacing={2} sx={{ mt: 2 }}>
                      <Stack direction="row" justifyContent="space-between" alignItems="center">
                        <Box>
                          <Typography variant="subtitle1">Compact lead cards</Typography>
                          <Typography color="text.secondary" variant="body2">
                            Fit more leads on screen without losing the critical proof points.
                          </Typography>
                        </Box>
                        <Switch
                          checked={settings.compactCards}
                          onChange={(event) =>
                            setSettings((current) => ({ ...current, compactCards: event.target.checked }))
                          }
                        />
                      </Stack>
                      <Stack direction="row" justifyContent="space-between" alignItems="center">
                        <Box>
                          <Typography variant="subtitle1">Only show contact-ready leads</Typography>
                          <Typography color="text.secondary" variant="body2">
                            Hide nurture-only accounts from the main working views.
                          </Typography>
                        </Box>
                        <Switch
                          checked={settings.showOnlyContactReady}
                          onChange={(event) =>
                            setSettings((current) => ({
                              ...current,
                              showOnlyContactReady: event.target.checked,
                            }))
                          }
                        />
                      </Stack>
                    </Stack>
                  </CardContent>
                </Card>
              </Grid>
              <Grid size={{ xs: 12, md: 6 }}>
                <Card>
                  <CardContent>
                    <Typography variant="h6">Pull history</Typography>
                    <Stack spacing={1.2} sx={{ mt: 2 }}>
                      {pullHistory.length > 0 ? (
                        pullHistory.slice(0, 8).map((item) => (
                          <Box
                            key={item.id}
                            sx={{
                              borderRadius: 2,
                              border: "1px solid rgba(15, 23, 42, 0.08)",
                              p: 1.5,
                            }}
                          >
                            <Typography variant="body2">
                              <strong>{item.status.toUpperCase()}</strong> · {formatPullTime(item.startedAt)}
                            </Typography>
                            <Typography color="text.secondary" variant="body2">
                              Duration: {item.durationSeconds ?? "N/A"}s
                            </Typography>
                            <Typography color="text.secondary" variant="body2">
                              {item.message}
                            </Typography>
                          </Box>
                        ))
                      ) : (
                        <Typography color="text.secondary" variant="body2">
                          No pulls recorded yet.
                        </Typography>
                      )}
                    </Stack>
                  </CardContent>
                </Card>
              </Grid>
              {badLeads.length > 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Card>
                    <CardContent>
                      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 2 }}>
                        <BlockRoundedIcon color="error" fontSize="small" />
                        <Typography variant="h6">Dismissed Leads ({badLeads.length})</Typography>
                      </Stack>
                      <Typography color="text.secondary" variant="body2" sx={{ mb: 2 }}>
                        These companies were manually dismissed. Dismissals are stored locally in your browser.
                      </Typography>
                      <Stack spacing={1}>
                        {badLeads.map((entry) => (
                          <Box
                            key={entry.leadId}
                            sx={{
                              display: "flex",
                              alignItems: "center",
                              gap: 1.5,
                              borderRadius: 2,
                              border: "1px solid rgba(15, 23, 42, 0.08)",
                              p: 1.5,
                            }}
                          >
                            <Box sx={{ flex: 1, minWidth: 0 }}>
                              <Typography variant="body2" fontWeight={600} noWrap>
                                {entry.company}
                              </Typography>
                              <Typography color="text.secondary" variant="caption">
                                {entry.industry} · {entry.city}, {entry.region} · {entry.leadTier}
                              </Typography>
                              <br />
                              <Typography color="error.main" variant="caption">
                                {BAD_LEAD_REASON_LABELS[entry.reason]} · {new Date(entry.markedAt).toLocaleDateString()}
                              </Typography>
                            </Box>
                            <Tooltip title="Restore lead">
                              <IconButton size="small" onClick={() => onUnmarkBadLead(entry.leadId)}>
                                <UndoRoundedIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          </Box>
                        ))}
                      </Stack>
                    </CardContent>
                  </Card>
                </Grid>
              ) : null}
              {naicsRules.length > 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Card>
                    <CardContent>
                      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                        <BlockRoundedIcon color="warning" fontSize="small" />
                        <Typography variant="h6">Industry Suppression Rules ({naicsRules.length})</Typography>
                        {autoSuppressedCount > 0 ? (
                          <Chip
                            label={`${autoSuppressedCount} leads auto-hidden`}
                            size="small"
                            color="warning"
                            variant="outlined"
                          />
                        ) : null}
                      </Stack>
                      <Typography color="text.secondary" variant="body2" sx={{ mb: 2 }}>
                        When you dismiss a hair salon or restaurant, a rule is added here that automatically
                        hides all other companies in the same NAICS subsector. Remove a rule to bring those
                        leads back.
                      </Typography>
                      <Stack spacing={1}>
                        {naicsRules.map((rule) => {
                          const hiddenCount = (liveLeads.length > 0 ? liveLeads : fallbackLeads).filter(
                            (l) => !badLeadIds.has(l.id) && l.naicsCode?.startsWith(rule.prefix),
                          ).length;
                          return (
                            <Box
                              key={rule.prefix}
                              sx={{
                                display: "flex",
                                alignItems: "center",
                                gap: 1.5,
                                borderRadius: 2,
                                border: "1px solid rgba(245, 158, 11, 0.25)",
                                bgcolor: "rgba(245, 158, 11, 0.04)",
                                p: 1.5,
                              }}
                            >
                              <Box sx={{ flex: 1, minWidth: 0 }}>
                                <Typography variant="body2" fontWeight={600}>
                                  NAICS {rule.prefix}xx — {rule.label}
                                </Typography>
                                <Typography color="text.secondary" variant="caption">
                                  Triggered by: {rule.exampleCompany} · {BAD_LEAD_REASON_LABELS[rule.reason]}
                                </Typography>
                                {hiddenCount > 0 ? (
                                  <>
                                    <br />
                                    <Typography color="warning.main" variant="caption">
                                      Hiding {hiddenCount} lead{hiddenCount !== 1 ? "s" : ""} with this NAICS code
                                    </Typography>
                                  </>
                                ) : null}
                              </Box>
                              <Tooltip title="Remove rule (restore similar leads)">
                                <IconButton size="small" onClick={() => onRemoveNaicsRule(rule.prefix)}>
                                  <UndoRoundedIcon fontSize="small" />
                                </IconButton>
                              </Tooltip>
                            </Box>
                          );
                        })}
                      </Stack>
                    </CardContent>
                  </Card>
                </Grid>
              ) : null}

              {/* Prospect List — named / known accounts */}
              {Object.keys(namedAccounts).length > 0 ? (
                <Grid size={{ xs: 12 }}>
                  <Card>
                    <CardContent>
                      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                        <Typography variant="h6">
                          Prospect List ({Object.keys(namedAccounts).length})
                        </Typography>
                        <Chip
                          label={`${dncKeys.size} DO NOT CONTACT hidden`}
                          size="small"
                          color="error"
                          variant="outlined"
                        />
                      </Stack>
                      <Typography color="text.secondary" variant="body2" sx={{ mb: 2 }}>
                        Companies flagged from your sales pipeline. "Not Interested" and "Unqualified" are automatically hidden from all lead views. Click the undo icon to remove an entry.
                      </Typography>
                      {(["meeting", "hot", "warm", "cold", "not_interested", "unqualified"] as AccountListStatus[]).map((status) => {
                        const entries = Object.entries(namedAccounts).filter(([, e]) => e.status === status);
                        if (entries.length === 0) return null;
                        return (
                          <Box key={status} sx={{ mb: 2 }}>
                            <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                              <AccountStatusBadge status={status} label={`${ACCOUNT_STATUS_LABELS[status]} (${entries.length})`} />
                            </Stack>
                            <Stack spacing={0.5}>
                              {entries.map(([key, entry]) => (
                                <Box
                                  key={key}
                                  sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 1,
                                    borderRadius: 1.5,
                                    border: "1px solid rgba(15,23,42,0.08)",
                                    px: 1.5,
                                    py: 0.75,
                                    bgcolor:
                                      status === "not_interested" || status === "unqualified"
                                        ? "rgba(239,68,68,0.04)"
                                        : status === "meeting"
                                        ? "rgba(34,197,94,0.04)"
                                        : "transparent",
                                  }}
                                >
                                  <Typography variant="body2" sx={{ flex: 1 }}>
                                    {entry.displayName}
                                  </Typography>
                                  <Tooltip title="Remove from list">
                                    <IconButton size="small" onClick={() => onRemoveNamedAccount(key)}>
                                      <UndoRoundedIcon fontSize="small" />
                                    </IconButton>
                                  </Tooltip>
                                </Box>
                              ))}
                            </Stack>
                          </Box>
                        );
                      })}
                    </CardContent>
                  </Card>
                </Grid>
              ) : null}
            </Grid>
          ) : null}
        </Stack>
      </Box>

      {/* Bad Lead confirmation dialog */}
      <Dialog
        open={badLeadDialogLead !== null}
        onClose={() => setBadLeadDialogLead(null)}
        maxWidth="xs"
        fullWidth
      >
        <DialogTitle>Mark as Not a Fit</DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb: 2 }}>
            <strong>{badLeadDialogLead?.company}</strong> will be hidden from the dashboard. Choose a reason:
          </Typography>
          <FormControl fullWidth size="small" sx={{ mb: 1.5 }}>
            <InputLabel>Reason</InputLabel>
            <Select
              label="Reason"
              value={badLeadReason}
              onChange={(e) => setBadLeadReason(e.target.value as BadLeadReason)}
            >
              {(Object.entries(BAD_LEAD_REASON_LABELS) as [BadLeadReason, string][]).map(([value, label]) => (
                <MenuItem key={value} value={value}>{label}</MenuItem>
              ))}
            </Select>
          </FormControl>
          {BROAD_SUPPRESS_REASONS.includes(badLeadReason) && badLeadDialogLead?.naicsCode && (
            <Alert severity="info" sx={{ mt: 1 }}>
              <Typography variant="caption">
                A suppression rule for NAICS <strong>{badLeadDialogLead.naicsCode.slice(0, 4)}xx</strong> (
                {badLeadDialogLead.industry}) will be created. Other companies in the same industry category
                will be automatically hidden. You can remove this rule in Settings.
              </Typography>
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setBadLeadDialogLead(null)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={() => badLeadDialogLead && onConfirmBadLead(badLeadDialogLead, badLeadReason)}
          >
            Dismiss Lead
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
