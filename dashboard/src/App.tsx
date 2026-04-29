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
  Typography,
} from "@mui/material";
import AssessmentRoundedIcon from "@mui/icons-material/AssessmentRounded";
import AutoAwesomeRoundedIcon from "@mui/icons-material/AutoAwesomeRounded";
import FilterAltRoundedIcon from "@mui/icons-material/FilterAltRounded";
import FlagRoundedIcon from "@mui/icons-material/FlagRounded";
import FolderSpecialRoundedIcon from "@mui/icons-material/FolderSpecialRounded";
import LocalFireDepartmentRoundedIcon from "@mui/icons-material/LocalFireDepartmentRounded";
import MenuRoundedIcon from "@mui/icons-material/MenuRounded";
import RefreshRoundedIcon from "@mui/icons-material/RefreshRounded";
import SearchRoundedIcon from "@mui/icons-material/SearchRounded";
import SettingsRoundedIcon from "@mui/icons-material/SettingsRounded";
import SourceRoundedIcon from "@mui/icons-material/SourceRounded";
import TuneRoundedIcon from "@mui/icons-material/TuneRounded";
import { leads as fallbackLeads } from "./data";
import { toViolationDetails } from "./oshaStandards";
import type { DashboardSettings, IncidentType, LeadRecord, NavView } from "./types";

const drawerWidth = 300;

const initialSettings: DashboardSettings = {
  compactCards: false,
  showOnlyContactReady: true,
  themeName: "signal",
};

type PullStatus = {
  id: string;
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
  { view: "hot-accounts", label: "Hot Accounts", icon: <LocalFireDepartmentRoundedIcon /> },
  { view: "research-needed", label: "Research Needed", icon: <FlagRoundedIcon /> },
  { view: "source-signals", label: "Source Signals", icon: <SourceRoundedIcon /> },
  { view: "saved-views", label: "Saved Views", icon: <FolderSpecialRoundedIcon /> },
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
];

function matchesSearch(lead: LeadRecord, query: string) {
  if (!query.trim()) {
    return true;
  }

  const normalizedViolations = toViolationDetails(lead.rawViolationCodes);
  const haystack = [
    lead.company,
    lead.city,
    lead.region,
    lead.industry,
    lead.reasonToContact,
    lead.whyNow,
    lead.recentInspectionContext,
    lead.incidentType,
    lead.incidentDate,
    lead.priority,
    lead.action,
    lead.matchedSources.join(" "),
    lead.rawViolationCodes.join(" "),
    normalizedViolations.map((item) => `${item.title} ${item.plainEnglish}`).join(" "),
  ]
    .join(" ")
    .toLowerCase();

  return haystack.includes(query.trim().toLowerCase());
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

  return (
    <Card sx={{ height: "100%" }}>
      <CardContent sx={{ p: compact ? 2 : 3 }}>
        <Stack direction="row" justifyContent="space-between" spacing={2}>
          <Box>
            <Typography variant="h6">{lead.company}</Typography>
            <Typography color="text.secondary" variant="body2">
              {lead.city}, {lead.region} · {lead.industry}
            </Typography>
          </Box>
          <Stack alignItems="flex-end" spacing={1}>
            <Chip color={getPriorityTone(lead.priority)} label={lead.priority} size="small" />
            <Chip color={getActionTone(lead.action)} label={lead.action} size="small" variant="outlined" />
          </Stack>
        </Stack>

        <Stack direction="row" flexWrap="wrap" gap={1} sx={{ mt: 2 }}>
          <Chip label={`Score ${lead.overallSalesScore}`} size="small" />
          <Chip label={`Evidence ${lead.eyewearEvidenceScore}`} size="small" />
          <Chip label={lead.needTier} size="small" />
          <Chip label={lead.employeeBand} size="small" />
          <Chip label={lead.incidentType} size="small" variant="outlined" />
          <Chip label={lead.incidentDate || "No incident date"} size="small" variant="outlined" />
          <Chip label={lead.accountStatus} size="small" variant="outlined" />
        </Stack>

        <Typography sx={{ mt: 2 }} variant="body2">
          {lead.reasonToContact}
        </Typography>
        <Typography sx={{ mt: 1 }} color="text.secondary" variant="body2">
          {lead.whyNow}
        </Typography>

        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle2">Violation and incident snapshot</Typography>
          <Typography sx={{ mt: 0.75 }} color="text.secondary" variant="body2">
            Incident date: {lead.incidentDate || "Unknown"} · Incident type: {lead.incidentType}
          </Typography>
          <Stack spacing={1} sx={{ mt: 1.25 }}>
            {normalizedViolations.length > 0 ? (
              normalizedViolations.map((violation) => (
                <Box key={`${lead.id}-${violation.code}`}>
                  <Typography variant="body2">
                    <strong>{violation.code}</strong> · {violation.title}
                  </Typography>
                  <Typography color="text.secondary" variant="body2">
                    {violation.plainEnglish}
                  </Typography>
                </Box>
              ))
            ) : (
              <Typography color="text.secondary" variant="body2">
                No mapped OSHA standard codes were available for this lead.
              </Typography>
            )}
          </Stack>
        </Box>

        <Stack direction="row" flexWrap="wrap" gap={1} sx={{ mt: 2 }}>
          {lead.matchedSources.map((source) => (
            <Chip key={source} label={source} size="small" variant="outlined" />
          ))}
          {lead.openViolations ? <Chip color="error" label="Open violations" size="small" /> : null}
          {lead.severeIncident ? <Chip color="warning" label="Severe incident" size="small" /> : null}
        </Stack>
      </CardContent>
    </Card>
  );
}

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
    <Card sx={{ mt: 1.5 }}>
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

function formatPullTime(isoTime?: string | null) {
  if (!isoTime) return "N/A";
  const date = new Date(isoTime);
  if (Number.isNaN(date.getTime())) return isoTime;
  return date.toLocaleString();
}

export default function App() {
  const [mobileOpen, setMobileOpen] = React.useState(false);
  const [activeView, setActiveView] = React.useState<NavView>("overview");
  const [query, setQuery] = React.useState("");
  const [regionFilter, setRegionFilter] = React.useState("All");
  const [priorityFilter, setPriorityFilter] = React.useState("All");
  const [sourceFilter, setSourceFilter] = React.useState("All");
  const [incidentFilter, setIncidentFilter] = React.useState("All");
  const [settings, setSettings] = React.useState(initialSettings);
  const [liveLeads, setLiveLeads] = React.useState<LeadRecord[]>([]);
  const [loadingLeads, setLoadingLeads] = React.useState(true);
  const [leadLoadError, setLeadLoadError] = React.useState<string | null>(null);
  const [pullStatus, setPullStatus] = React.useState<PullStatus | null>(null);
  const [pullHistory, setPullHistory] = React.useState<PullHistoryItem[]>([]);
  const [triggeringPull, setTriggeringPull] = React.useState(false);

  const loadLeads = React.useCallback(async () => {
    setLoadingLeads(true);
    setLeadLoadError(null);
    try {
      const response = await fetch("/api/leads");
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Failed to load leads");
      }
      setLiveLeads(payload.leads || []);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load leads";
      setLeadLoadError(message);
      setLiveLeads([]);
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
      loadLeads();
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

  const onSaveLeadOutcome = async (leadId: string, outreachStatus: OutreachStatus, outreachNotes: string) => {
    try {
      const response = await fetch("/api/lead-outcomes", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ leadId, outreachStatus, outreachNotes }),
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Failed to save outcome");
      }
      setLiveLeads((current) =>
        current.map((lead) =>
          lead.id === leadId
            ? {
                ...lead,
                outreachStatus,
                outreachNotes,
                outreachUpdatedAt: payload.outcome?.outreachUpdatedAt || new Date().toISOString(),
                accountStatus: payload.outcome?.accountStatus || lead.accountStatus,
              }
            : lead,
        ),
      );
    } catch (error) {
      setLeadLoadError(error instanceof Error ? error.message : "Failed to save outreach update");
    } finally {
    }
  };

  const leadData = liveLeads.length > 0 ? liveLeads : fallbackLeads;

  const visibleLeads = leadData.filter((lead) => {
    if (!matchesSearch(lead, query)) return false;
    if (regionFilter !== "All" && lead.region !== regionFilter) return false;
    if (priorityFilter !== "All" && lead.priority !== priorityFilter) return false;
    if (sourceFilter !== "All" && !lead.matchedSources.includes(sourceFilter)) return false;
    if (incidentFilter !== "All" && lead.incidentType !== incidentFilter) return false;
    if (settings.showOnlyContactReady && !["Call Now", "Call This Week", "Research Then Call"].includes(lead.action)) {
      return false;
    }
    return true;
  });

  const hotAccounts = visibleLeads.filter((lead) => lead.action === "Call Now" || lead.priority === "P0 Ideal");
  const researchNeeded = visibleLeads.filter(
    (lead) => lead.action === "Research Then Call" || lead.priority === "P2 Research",
  );
  const sourceSignalRows = visibleLeads.flatMap((lead) =>
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
  );

  const recentIncidents = visibleLeads.filter((lead) => {
    if (!lead.incidentDate) return false;
    const daysSinceIncident = Math.floor(
      (Date.now() - new Date(`${lead.incidentDate}T00:00:00`).getTime()) / (1000 * 60 * 60 * 24),
    );
    return daysSinceIncident <= 30;
  });

  const navCounts: Record<NavView, number | string> = {
    overview: visibleLeads.length,
    "lead-queue": visibleLeads.length,
    "hot-accounts": hotAccounts.length,
    "research-needed": researchNeeded.length,
    "source-signals": sourceSignalRows.length,
    "saved-views": 4,
    settings: "",
  };

  const latestPull = pullHistory[0] || null;
  const attemptedCount = visibleLeads.filter((lead) => lead.outreachStatus === "attempted").length;
  const connectedCount = visibleLeads.filter((lead) => lead.outreachStatus === "connected").length;
  const wonCount = visibleLeads.filter((lead) => lead.outreachStatus === "won").length;
  const lostCount = visibleLeads.filter((lead) => lead.outreachStatus === "lost").length;

  const drawer = (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Box sx={{ px: 3, pt: 3, pb: 2 }}>
        <Stack direction="row" spacing={1.5} alignItems="center">
          <Avatar sx={{ bgcolor: "#c96f31", color: "#fff" }}>
            <AutoAwesomeRoundedIcon />
          </Avatar>
          <Box>
            <Typography variant="h6">Lead Signal Desk</Typography>
            <Typography sx={{ opacity: 0.78 }} variant="body2">
              Daily sales pipeline navigator
            </Typography>
          </Box>
        </Stack>
      </Box>
      <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />
      <List sx={{ px: 1.5, py: 2 }}>
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
              "&.Mui-selected": {
                backgroundColor: "rgba(255,255,255,0.14)",
              },
            }}
          >
            <ListItemIcon sx={{ color: "inherit", minWidth: 40 }}>{item.icon}</ListItemIcon>
            <ListItemText primary={item.label} />
            {navCounts[item.view] !== "" ? <span className="nav-pill">{navCounts[item.view]}</span> : null}
          </ListItemButton>
        ))}
      </List>

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
    </Box>
  );

  return (
    <Box sx={{ display: "flex", minHeight: "100vh" }}>
      <AppBar
        color="inherit"
        elevation={0}
        position="fixed"
        sx={{
          width: { md: `calc(100% - ${drawerWidth}px)` },
          ml: { md: `${drawerWidth}px` },
          borderBottom: "1px solid rgba(31, 41, 55, 0.08)",
          backdropFilter: "blur(18px)",
          backgroundColor: alpha("#f4efe7", 0.82),
        }}
      >
        <Toolbar sx={{ gap: 2 }}>
          <IconButton onClick={() => setMobileOpen(true)} sx={{ display: { md: "none" } }}>
            <MenuRoundedIcon />
          </IconButton>
          <TextField
            fullWidth
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
          <Button
            color="secondary"
            disabled={triggeringPull || pullStatus?.status === "running"}
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
            Refresh Leads
          </Button>
          <Button startIcon={<TuneRoundedIcon />} variant="contained" onClick={() => setActiveView("settings")}>
            Settings
          </Button>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: drawerWidth }, flexShrink: { md: 0 } }}>
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={() => setMobileOpen(false)}
          ModalProps={{ keepMounted: true }}
          sx={{ display: { xs: "block", md: "none" }, "& .MuiDrawer-paper": { width: drawerWidth } }}
        >
          {drawer}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: "none", md: "block" },
            "& .MuiDrawer-paper": { width: drawerWidth, boxSizing: "border-box" },
          }}
          open
        >
          {drawer}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: { md: `calc(100% - ${drawerWidth}px)` },
          px: { xs: 2, md: 4 },
          py: 4,
        }}
      >
        <Toolbar />

        <Stack spacing={3}>
          {leadLoadError ? <Alert severity="warning">Live load issue: {leadLoadError}. Showing fallback sample data.</Alert> : null}
          {loadingLeads ? <Alert severity="info">Loading live leads from BigQuery...</Alert> : null}
          {pullStatus?.status === "running" ? (
            <Alert severity="info">Pipeline pull started at {formatPullTime(pullStatus.startedAt)}. This can take several minutes.</Alert>
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
              {activeView === "hot-accounts" && "Hot Accounts"}
              {activeView === "research-needed" && "Research Needed"}
              {activeView === "source-signals" && "Source Signals"}
              {activeView === "saved-views" && "Saved Views"}
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
              <Grid container spacing={2}>
                <Grid size={{ xs: 12, md: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Region</InputLabel>
                    <Select label="Region" value={regionFilter} onChange={(event) => setRegionFilter(event.target.value)}>
                      <MenuItem value="All">All regions</MenuItem>
                      <MenuItem value="San Diego">San Diego</MenuItem>
                      <MenuItem value="Bay Area">Bay Area</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Priority</InputLabel>
                    <Select
                      label="Priority"
                      value={priorityFilter}
                      onChange={(event) => setPriorityFilter(event.target.value)}
                    >
                      <MenuItem value="All">All priorities</MenuItem>
                      <MenuItem value="P0 Ideal">P0 Ideal</MenuItem>
                      <MenuItem value="P1 Active">P1 Active</MenuItem>
                      <MenuItem value="P2 Research">P2 Research</MenuItem>
                      <MenuItem value="P3 Monitor">P3 Monitor</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Source</InputLabel>
                    <Select label="Source" value={sourceFilter} onChange={(event) => setSourceFilter(event.target.value)}>
                      <MenuItem value="All">All sources</MenuItem>
                      <MenuItem value="OSHA">OSHA</MenuItem>
                      <MenuItem value="FDA">FDA</MenuItem>
                      <MenuItem value="EPA">EPA</MenuItem>
                      <MenuItem value="NIH">NIH</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
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
              </Grid>
            </CardContent>
          </Card>

          {activeView === "overview" ? (
            <>
              <Grid container spacing={2.5}>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Contact Ready"
                    value={`${visibleLeads.length}`}
                    supporting="Leads remaining after your active filters."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Call Now"
                    value={`${hotAccounts.length}`}
                    supporting="Accounts with the strongest urgency and evidence."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Open Violations"
                    value={`${visibleLeads.filter((lead) => lead.openViolations).length}`}
                    supporting="Accounts still carrying active OSHA pressure."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Multi-source Matches"
                    value={`${visibleLeads.filter((lead) => lead.matchedSources.length > 1).length}`}
                    supporting="Leads reinforced by more than one data source."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard
                    label="Recent Incidents"
                    value={`${recentIncidents.length}`}
                    supporting="Leads with incident dates inside the last 30 days."
                  />
                </Grid>
                <Grid size={{ xs: 12, md: 3 }}>
                  <StatCard label="Attempted" value={`${attemptedCount}`} supporting="Outreach attempts logged." />
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
                      <Typography variant="h6">Best Next Calls</Typography>
                      <Stack spacing={2} sx={{ mt: 2 }}>
                        {hotAccounts.slice(0, 4).map((lead) => (
                          <Box key={lead.id}>
                            <LeadCard lead={lead} compact={settings.compactCards} />
                            <OutreachCard lead={lead} onSave={onSaveLeadOutcome} />
                          </Box>
                        ))}
                      </Stack>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid size={{ xs: 12, lg: 5 }}>
                  <Card sx={{ height: "100%" }}>
                    <CardContent>
                      <Typography variant="h6">What belongs in the navigation</Typography>
                      <Stack spacing={1.5} sx={{ mt: 2 }}>
                        <Typography variant="body2">
                          `Lead Queue` for the full working list with search, code, incident, and source filters.
                        </Typography>
                        <Typography variant="body2">
                          `Hot Accounts` for immediate outreach so the top of funnel stays visible all day.
                        </Typography>
                        <Typography variant="body2">
                          `Research Needed` for accounts worth keeping warm while owner mapping or enrichment catches up.
                        </Typography>
                        <Typography variant="body2">
                          `Source Signals` so you can inspect which OSHA code or supporting source is driving the lead.
                        </Typography>
                        <Typography variant="body2">
                          `Saved Views` for team presets like severe injuries, prescription safety, or Bay Area P1s.
                        </Typography>
                        <Typography variant="body2">
                          `Settings` for compact mode, default filters, and what "contact ready" should include.
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
              {visibleLeads.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <LeadCard lead={lead} compact={settings.compactCards} />
                  <OutreachCard lead={lead} onSave={onSaveLeadOutcome} />
                </Grid>
              ))}
            </Grid>
          ) : null}

          {activeView === "hot-accounts" ? (
            <Grid container spacing={2.5}>
              {hotAccounts.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <LeadCard lead={lead} compact={settings.compactCards} />
                  <OutreachCard lead={lead} onSave={onSaveLeadOutcome} />
                </Grid>
              ))}
            </Grid>
          ) : null}

          {activeView === "research-needed" ? (
            <Grid container spacing={2.5}>
              {researchNeeded.map((lead) => (
                <Grid key={lead.id} size={{ xs: 12, lg: 6 }}>
                  <LeadCard lead={lead} compact={settings.compactCards} />
                  <OutreachCard lead={lead} onSave={onSaveLeadOutcome} />
                </Grid>
              ))}
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

          {activeView === "saved-views" ? (
            <Grid container spacing={2.5}>
              {[
                {
                  title: "Immediate Calls",
                  note: "P0 and call-now leads with open violations or severe incidents.",
                },
                {
                  title: "Prescription Safety Queue",
                  note: "Accounts with 1910.133(a)(3) or other prescription-related violation evidence.",
                },
                {
                  title: "Research Before Outreach",
                  note: "P2 accounts with good evidence but unclear ownership or timing.",
                },
                {
                  title: "Multi-source Proof",
                  note: "Accounts matched by more than one data source for stronger stories.",
                },
              ].map((view) => (
                <Grid key={view.title} size={{ xs: 12, md: 6 }}>
                  <Card>
                    <CardContent>
                      <Typography variant="h6">{view.title}</Typography>
                      <Typography sx={{ mt: 1 }} color="text.secondary" variant="body2">
                        {view.note}
                      </Typography>
                      <Button sx={{ mt: 2 }} variant="outlined">
                        Open View
                      </Button>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
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
            </Grid>
          ) : null}
        </Stack>
      </Box>
    </Box>
  );
}
