import express from "express";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dashboardRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(dashboardRoot, "..");
const bqCommand = "bq";
const runtimeDir = path.resolve(dashboardRoot, ".runtime");
const historyFile = path.resolve(runtimeDir, "pull-history.json");
const outcomesFile = path.resolve(runtimeDir, "lead-outcomes.json");
const leadsSnapshotFile = path.resolve(runtimeDir, "leads-cache.json");
const port = Number(process.env.DASHBOARD_API_PORT || 8787);
const LEADS_CACHE_TTL_MS = 45_000;
const BAY_AREA_ANCHOR_LAT = 37.6776;
const BAY_AREA_ANCHOR_LON = -122.1297;
const BAY_AREA_RADIUS_MILES = 50;
// 6780 Miramar Rd, San Diego, CA 92121
const SAN_DIEGO_ANCHOR_LAT = 32.8730;
const SAN_DIEGO_ANCHOR_LON = -117.1604;
const SAN_DIEGO_RADIUS_MILES = 50;
const SAN_DIEGO_BORDER_CITIES = [
  "SAN DIEGO",
  "CHULA VISTA",
  "NATIONAL CITY",
  "LA MESA",
  "EL CAJON",
  "SANTEE",
  "LEMON GROVE",
  "IMPERIAL BEACH",
  "CORONADO",
  "POWAY",
  "ESCONDIDO",
  "VISTA",
  "OCEANSIDE",
  "CARLSBAD",
  "SAN MARCOS",
  "ENCINITAS",
  "DEL MAR",
  "SOLANA BEACH",
];

const app = express();
app.use(express.json());

let currentPull = null;
let runtimeEnvCache = null;
let leadsCache = {
  leads: [],
  generatedAt: null,
  totalAvailable: null,
  cacheUntil: 0,
};

function parseDotEnv(rawText) {
  const map = new Map();
  for (const rawLine of rawText.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#") || !line.includes("=")) {
      continue;
    }
    const idx = line.indexOf("=");
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim().replace(/^['"]|['"]$/g, "");
    map.set(key, value);
  }
  return map;
}

async function loadPipelineConfig() {
  const env = await loadRuntimeEnvMap();

  return {
    projectId: env.get("PROJECT_ID") || "cold-lead-pipeline-dashboard",
    dataset: env.get("BQ_DATASET") || "osha_raw",
  };
}

async function loadRuntimeEnvMap() {
  const localPath = path.resolve(repoRoot, ".env.local");
  const fallbackPath = path.resolve(repoRoot, ".env");
  const dotenvPath = localPath;
  try {
    const text = await fs.readFile(dotenvPath, "utf8");
    return parseDotEnv(text);
  } catch (_error) {
    try {
      const fallbackText = await fs.readFile(fallbackPath, "utf8");
      return parseDotEnv(fallbackText);
    } catch (_fallbackError) {
      return new Map();
    }
  }
}

function toRuntimeEnv(dotenvMap) {
  const env = { ...process.env };
  for (const [key, value] of dotenvMap.entries()) {
    if (!env[key] || String(env[key]).trim() === "") {
      env[key] = value;
    }
  }
  return env;
}

function isInlineJson(value) {
  const trimmed = String(value || "").trim();
  return trimmed.startsWith("{") && trimmed.endsWith("}");
}

function maybeDecodeBase64Json(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return null;
  }
  try {
    const decoded = Buffer.from(trimmed, "base64").toString("utf8").trim();
    if (decoded.startsWith("{") && decoded.endsWith("}")) {
      return decoded;
    }
    return null;
  } catch (_error) {
    return null;
  }
}

async function resolveServiceAccountPath(runtimeEnv) {
  await ensureRuntimeDir();

  const pathCandidates = [
    runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS,
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_PATH,
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_FILE,
  ];
  const legacyValue = runtimeEnv.BigQuery_Service_Account_Key;
  if (legacyValue && !isInlineJson(legacyValue) && !maybeDecodeBase64Json(legacyValue)) {
    pathCandidates.push(legacyValue);
  }

  for (const candidate of pathCandidates) {
    if (!candidate || String(candidate).trim() === "") {
      continue;
    }
    const resolved = path.isAbsolute(candidate)
      ? candidate
      : path.resolve(repoRoot, String(candidate));
    try {
      await fs.access(resolved);
      return resolved;
    } catch (_error) {
      // Continue to inline fallback candidates.
    }
  }

  const inlineCandidates = [
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_JSON,
    runtimeEnv.GOOGLE_SERVICE_ACCOUNT_JSON,
    runtimeEnv.GCP_SERVICE_ACCOUNT_JSON,
    runtimeEnv.BigQuery_Service_Account_Key,
  ];

  for (const candidate of inlineCandidates) {
    if (!candidate || String(candidate).trim() === "") {
      continue;
    }

    const maybeJson = isInlineJson(candidate)
      ? String(candidate).trim()
      : maybeDecodeBase64Json(candidate);
    if (!maybeJson) {
      continue;
    }

    const keyPath = path.resolve(runtimeDir, "gcp-service-account.json");
    await fs.writeFile(keyPath, maybeJson, "utf8");
    return keyPath;
  }

  return null;
}

async function getRuntimeEnv() {
  if (runtimeEnvCache) {
    return runtimeEnvCache;
  }

  const dotenvMap = await loadRuntimeEnvMap();
  const runtimeEnv = toRuntimeEnv(dotenvMap);
  const serviceAccountPath = await resolveServiceAccountPath(runtimeEnv);
  if (serviceAccountPath && (!runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS || runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS.trim() === "")) {
    runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS = serviceAccountPath;
  }

  runtimeEnvCache = runtimeEnv;
  return runtimeEnv;
}

async function resolvePythonCommand() {
  const venvPython = path.resolve(repoRoot, ".venv", "Scripts", "python.exe");
  try {
    await fs.access(venvPython);
    return venvPython;
  } catch (_error) {
    return "python";
  }
}

async function ensureRuntimeDir() {
  await fs.mkdir(runtimeDir, { recursive: true });
}

async function readHistory() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(historyFile, "utf8");
    const rows = JSON.parse(payload);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    return [];
  }
}

async function writeHistory(history) {
  await ensureRuntimeDir();
  await fs.writeFile(historyFile, JSON.stringify(history, null, 2), "utf8");
}

async function readOutcomes() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(outcomesFile, "utf8");
    const parsed = JSON.parse(payload);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_error) {
    return {};
  }
}

async function writeOutcomes(outcomes) {
  await ensureRuntimeDir();
  await fs.writeFile(outcomesFile, JSON.stringify(outcomes, null, 2), "utf8");
}

async function readLeadsSnapshot() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(leadsSnapshotFile, "utf8");
    const parsed = JSON.parse(payload);
    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.leads)) {
      return null;
    }
    return {
      leads: parsed.leads,
      generatedAt: typeof parsed.generatedAt === "string" ? parsed.generatedAt : new Date().toISOString(),
      totalAvailable: Number.isFinite(Number(parsed.totalAvailable)) ? Number(parsed.totalAvailable) : null,
    };
  } catch (_error) {
    return null;
  }
}

async function writeLeadsSnapshot(leads, generatedAt, totalAvailable = null) {
  await ensureRuntimeDir();
  await fs.writeFile(
    leadsSnapshotFile,
    JSON.stringify({ leads, generatedAt, totalAvailable }, null, 2),
    "utf8",
  );
}

function normalizeCodes(rawStandards) {
  if (!rawStandards) {
    return [];
  }

  const tokens = String(rawStandards)
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);

  const codeRegex = /\b\d{4}\.\d+(?:\([^)]+\))*\b/g;
  const codes = [];
  for (const token of tokens) {
    const matches = token.match(codeRegex);
    if (matches) {
      for (const code of matches) {
        if (!codes.includes(code)) {
          codes.push(code);
        }
      }
    }
  }
  return codes;
}

function inferIncidentType(row) {
  if (String(row["lead_type"] || "").toLowerCase() === "profile_fit") {
    return "Profile Fit";
  }

  // v3 schema: derive from new score/count fields
  const eyeInjury = Number(row["eye_injury_count"] || 0) > 0;
  const prescription = Number(row["prescription_violation_count"] || 0) > 0;
  const eyeViolation = Number(row["eye_violation_count"] || 0) > 0;
  const openViolation = Number(row["open_eye_violation_count"] || 0) > 0;
  const generalPpe = Number(row["general_ppe_violation_count"] || 0) > 0;

  if (eyeInjury) return "Severe Injury";
  if (prescription) return "Prescription Safety";
  if (openViolation) return "Impact Hazard";
  if (eyeViolation) return "Impact Hazard";
  if (generalPpe) return "General PPE";
  return "General PPE";
}

function resolveIncidentDate(row) {
  // v3 schema uses snake_case column names
  if (row["last_eye_injury_date"]) {
    return { value: row["last_eye_injury_date"], source: "accident" };
  }
  if (row["last_violation_event_date"]) {
    return { value: row["last_violation_event_date"], source: "violation-event" };
  }
  if (row["last_violation_date"]) {
    return { value: row["last_violation_date"], source: "violation-event" };
  }
  if (row["close_case_date"]) {
    return { value: row["close_case_date"], source: "case-close" };
  }
  if (row["open_case_date"]) {
    return { value: row["open_case_date"], source: "case-open" };
  }
  return { value: null, source: "unknown" };
}

function normalizeNaicsCode(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length >= 2 ? digits : "";
}

function industryFromNaics(naicsCode) {
  const code = normalizeNaicsCode(naicsCode);
  if (!code) return "";

  const exact = {
    "211120": "Crude Petroleum Extraction",
    "221122": "Electric Power Distribution",
    "311615": "Poultry Processing",
    "312120": "Breweries",
    "324110": "Petroleum Refineries",
    "325199": "Basic Organic Chemical Manufacturing",
    "325412": "Pharmaceutical Preparation Manufacturing",
    "325413": "In-Vitro Diagnostic Substance Manufacturing",
    "325414": "Biological Product Manufacturing",
    "325510": "Paint and Coating Manufacturing",
    "325611": "Soap and Detergent Manufacturing",
    "325998": "Chemical Product Manufacturing",
    "326199": "Plastic Product Manufacturing",
    "332710": "Machine Shops",
    "332994": "Small Arms Manufacturing",
    "333120": "Construction Machinery Manufacturing",
    "333314": "Optical Instrument and Lens Manufacturing",
    "333415": "HVAC and Commercial Refrigeration Equipment Manufacturing",
    "334413": "Semiconductor and Related Device Manufacturing",
    "334416": "Capacitor, Resistor, Coil, Transformer Manufacturing",
    "334510": "Electromedical and Electrotherapeutic Apparatus Manufacturing",
    "334516": "Analytical Laboratory Instrument Manufacturing",
    "334519": "Measuring and Controlling Device Manufacturing",
    "336411": "Aircraft Manufacturing",
    "336412": "Aircraft Engine and Engine Parts Manufacturing",
    "336413": "Other Aircraft Parts and Equipment Manufacturing",
    "339112": "Surgical and Medical Instrument Manufacturing",
    "339113": "Surgical Appliance and Supplies Manufacturing",
    "423450": "Medical, Dental, and Hospital Equipment Wholesalers",
    "493110": "General Warehousing and Storage",
    "541380": "Testing Laboratories",
    "541714": "Research and Development in Biotechnology",
    "562910": "Remediation Services",
  };

  if (exact[code]) {
    return `${exact[code]} (NAICS ${code})`;
  }

  const prefixRules = [
    ["3254", "Pharmaceutical and Medicine Manufacturing"],
    ["325", "Chemical Manufacturing"],
    ["334", "Computer and Electronic Product Manufacturing"],
    ["3364", "Aerospace Product and Parts Manufacturing"],
    ["336", "Transportation Equipment Manufacturing"],
    ["3391", "Medical Equipment and Supplies Manufacturing"],
    ["339", "Miscellaneous Manufacturing"],
    ["333", "Machinery Manufacturing"],
    ["332", "Fabricated Metal Product Manufacturing"],
    ["311", "Food Manufacturing"],
    ["312", "Beverage and Tobacco Product Manufacturing"],
    ["493", "Warehousing and Storage"],
    ["5417", "Scientific Research and Development Services"],
    ["541", "Professional, Scientific, and Technical Services"],
    ["562", "Waste Management and Remediation Services"],
    ["42", "Merchant Wholesalers"],
    ["23", "Construction"],
    ["31", "Manufacturing"],
    ["32", "Manufacturing"],
    ["33", "Manufacturing"],
  ];

  for (const [prefix, label] of prefixRules) {
    if (code.startsWith(prefix)) {
      return `${label} (NAICS ${code})`;
    }
  }

  return "";
}

function resolveIndustryLabel(row) {
  const naicsLabel = industryFromNaics(row["naics_code"]);
  if (naicsLabel) {
    return naicsLabel;
  }
  return String(row["industry_segment"] || "").trim() || "Unknown Industry";
}

function normalizeCaliforniaRegion(row) {
  const state = String(row["site_state"] || "").trim().toUpperCase();
  const city = String(row["site_city"] || "").trim().toUpperCase();
  const rawRegion = String(row["region"] || "").trim().toUpperCase();

  if (state && state !== "CA") {
    return String(row["region"] || "Other").trim() || "Other";
  }

  // Primary: use geo_match_source which is distance-based and authoritative
  const geoSrc = String(row["geo_match_source"] || "").toLowerCase();
  if (geoSrc === "bay_radius") return "Northern California";
  if (geoSrc === "san_diego_area") return "Southern California";
  if (geoSrc === "bay_radius|san_diego_area") {
    // Overlap edge case: assign to whichever anchor is closer
    const bayDist = Number(row["bay_area_distance_miles"] ?? 999999);
    const sdDist = Number(row["san_diego_distance_miles"] ?? 999999);
    return bayDist <= sdDist ? "Northern California" : "Southern California";
  }

  // Fallback: city-based for records without geo_match_source
  const northernCitiesFallback = new Set([
    "SAN FRANCISCO", "OAKLAND", "BERKELEY", "RICHMOND", "SAN JOSE", "FREMONT",
    "PALO ALTO", "MOUNTAIN VIEW", "SUNNYVALE", "REDWOOD CITY", "SAN MATEO",
    "MENLO PARK", "BURLINGAME", "SOUTH SAN FRANCISCO", "SANTA CLARA",
    "WALNUT CREEK", "CONCORD", "ANTIOCH", "PITTSBURG", "BRENTWOOD",
    "LIVERMORE", "HAYWARD", "SAN LEANDRO", "SAN LORENZO",
  ]);
  const southernCitiesFallback = new Set([
    "SAN DIEGO", "CHULA VISTA", "EL CAJON", "SANTEE", "LA MESA",
    "NATIONAL CITY", "IMPERIAL BEACH", "CORONADO", "POWAY", "ESCONDIDO",
    "VISTA", "OCEANSIDE", "CARLSBAD", "SAN MARCOS", "ENCINITAS", "DEL MAR",
    "LOS ANGELES", "LONG BEACH", "ANAHEIM", "IRVINE", "SANTA ANA",
    "RIVERSIDE", "SAN BERNARDINO",
  ]);

  if (northernCitiesFallback.has(city)) return "Northern California";
  if (southernCitiesFallback.has(city)) return "Southern California";

  if (rawRegion.includes("BAY") || rawRegion.includes("NORTH")) return "Northern California";
  if (rawRegion.includes("LOS ANGELES") || rawRegion.includes("SOUTH") || rawRegion.includes("SAN DIEGO")) return "Southern California";

  return "Northern California";
}

function toLeadRecord(row) {
  const incidentDateInfo = resolveIncidentDate(row);
  const incidentDateIso = incidentDateInfo.value
    ? String(incidentDateInfo.value).slice(0, 10)
    : "";
  const now = Date.now();
  const lastTouchedDays = incidentDateIso
    ? Math.max(0, Math.floor((now - new Date(`${incidentDateIso}T00:00:00Z`).getTime()) / 86400000))
    : 0;

  const tier = row["lead_tier"] || "P3 Industry Fit";
  const leadType = String(row["lead_type"] || (row["inspection_id"] ? "incident" : "profile_fit"));
  const finalScore = Number(row["final_score"] || 0);
  const isCityLicenseLead = !row["inspection_id"];

  // Map lead tier to legacy priority / action labels
  const priorityMap = {
    "P0 Hot Eye":       "P0 Ideal",
    "P1 Eye Violation": "P1 Active",
    "P2 PPE Opportunity": "P2 Research",
    "P3 Industry Fit":  "P3 Monitor",
  };
  const actionMap = {
    "P0 Hot Eye":       "Ideal Call Now",
    "P1 Eye Violation": "Call Now",
    "P2 PPE Opportunity": finalScore >= 30 ? "Call This Week" : "Research Then Call",
    "P3 Industry Fit":  "Monitor / Nurture",
  };

  const eyeInjuryDescriptions = String(row["eye_injury_descriptions"] || "")
    .split("|")
    .map((s) => s.trim())
    .filter(Boolean);

  const emphasisCodes = String(row["emphasis_code_list"] || "")
    .split("|")
    .map((s) => s.trim())
    .filter(Boolean);

  return {
    id: `lead-${row["inspection_id"] || row["account_name"] || Math.random().toString(16).slice(2)}`,
    company: row["account_name"] || "Unknown Company",
    region: normalizeCaliforniaRegion(row),
    county: row["county"] || "",
    distanceFromMiramarMiles:
      row["san_diego_distance_miles"] != null
        ? Number(row["san_diego_distance_miles"])
        : (row["distance_from_miramar_miles"] != null ? Number(row["distance_from_miramar_miles"]) : null),
    bayAreaDistanceMiles:
      row["bay_area_distance_miles"] === null || row["bay_area_distance_miles"] === undefined
        ? null
        : Number(row["bay_area_distance_miles"]),
    isWithinBayArea50Mi:
      row["is_within_bay_area_50mi"] === true
      || String(row["is_within_bay_area_50mi"] || "").toLowerCase() === "true",
    isSanDiegoArea:
      row["is_san_diego_area"] === true
      || String(row["is_san_diego_area"] || "").toLowerCase() === "true",
    geoMatchSource: String(row["geo_match_source"] || "none"),
    leadType,
    qualifiesIncident3Year:
      row["qualifies_incident_3yr"] === true
      || String(row["qualifies_incident_3yr"] || "").toLowerCase() === "true",
    city: row["site_city"] || "",
    industry: resolveIndustryLabel(row),
    ownerType: row["ownership_type"] || "",

    // v3 scores
    eyeLeadScore: Number(row["eye_lead_score"] || 0),
    ppeScore: Number(row["ppe_score"] || 0),
    finalScore,
    leadTier: tier,

    // legacy compat fields for components that still reference them
    overallSalesScore: finalScore,
    eyewearEvidenceScore: Number(row["eye_lead_score"] || 0),
    priority: priorityMap[tier] || "P3 Monitor",
    action: actionMap[tier] || "Monitor / Nurture",

    // v3 eye injury evidence
    eyeInjuryCount: Number(row["eye_injury_count"] || 0),
    fatalityCount: Number(row["fatality_count"] || 0),
    faceHeadInjuryCount: Number(row["face_head_injury_count"] || 0),
    eyeInjuryDescriptions,

    // v3 violation evidence
    eyeViolationCount: Number(row["eye_violation_count"] || 0),
    prescriptionViolationCount: Number(row["prescription_violation_count"] || 0),
    openEyeViolationCount: Number(row["open_eye_violation_count"] || 0),
    generalPpeViolationCount: Number(row["general_ppe_violation_count"] || 0),
    openGeneralPpeViolationCount: Number(row["open_general_ppe_violation_count"] || 0),
    willfulViolationCount: Number(row["willful_violation_count"] || 0),
    repeatViolationCount: Number(row["repeat_violation_count"] || 0),
    totalCurrentPenalty: Number(row["total_current_penalty"] || 0),

    // v3 enrichment signals
    violationEventCount: Number(row["violation_event_count"] || 0),
    contestedViolationCount: Number(row["contested_violation_count"] || 0),
    eyeEmphasisCount: Number(row["eye_emphasis_count"] || 0),
    emphasisCodes,
    relatedInspectionCount: Number(row["related_inspection_count"] || 0),
    formalFollowupCount: Number(row["formal_followup_count"] || 0),
    totalInspectionCount: Number(row["total_inspection_count"] || 0),

    rawViolationCodes: normalizeCodes(row["standards_cited"]),
    openViolations: row["has_open_violations"] === true || String(row["has_open_violations"] || "").toLowerCase() === "true",

    pitchRecommendation: row["pitch_recommendation"] || "",
    employeeBand: row["employee_band"] || "Unknown",

    // date / incident
    incidentDate: incidentDateIso,
    incidentDateSource: incidentDateInfo.source,
    incidentType: inferIncidentType(row),
    lastTouchedDays,
    accountStatus: "New",

    // v3 direct date fields
    openCaseDate: row["open_case_date"] ? String(row["open_case_date"]).slice(0, 10) : "",
    closeCaseDate: row["close_case_date"] ? String(row["close_case_date"]).slice(0, 10) : "",
    lastEyeInjuryDate: row["last_eye_injury_date"] ? String(row["last_eye_injury_date"]).slice(0, 10) : "",

    // legacy compat
    needTier: tier === "P0 Hot Eye" || tier === "P1 Eye Violation" ? "Direct Need"
      : tier === "P2 PPE Opportunity" ? "Probable Need" : "Fit Only",
    matchedSources: isCityLicenseLead ? ["City License"] : ["OSHA"],
    reasonToContact: row["pitch_recommendation"] || "",
    whyNow: "",
    recentInspectionContext: "",
    severeIncident: Number(row["eye_injury_count"] || 0) > 0,
  };
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const child = spawn(command, args, {
      cwd: options.cwd || repoRoot,
      env: { ...process.env, ...(options.env || {}) },
      shell: options.shell === true,
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdout = [];
    const stderr = [];
    let timeoutHandle = null;

    const finishWithError = (message) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
      reject(new Error(message));
    };

    if (typeof options.timeoutMs === "number" && options.timeoutMs > 0) {
      timeoutHandle = setTimeout(() => {
        const out = stdout.join("").trim();
        const err = stderr.join("").trim();
        const tail = `${out}\n${err}`.trim().split(/\r?\n/).slice(-20).join("\n");
        child.kill("SIGTERM");
        finishWithError(
          `Command timed out after ${options.timeoutMs}ms: ${command} ${args.join(" ")}\n${tail}`,
        );
      }, options.timeoutMs);
    }

    child.stdout.on("data", (chunk) => stdout.push(String(chunk)));
    child.stderr.on("data", (chunk) => stderr.push(String(chunk)));

    if (typeof options.input === "string") {
      child.stdin.write(options.input);
    }
    child.stdin.end();

    child.on("error", (error) => {
      finishWithError(error instanceof Error ? error.message : String(error));
    });
    child.on("close", (code) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
      const out = stdout.join("");
      const err = stderr.join("");
      if (code !== 0) {
        reject(new Error(err || out || `${command} exited with code ${code}`));
        return;
      }
      resolve(out);
    });
  });
}

async function ensureBigQueryAuth() {
  const cfg = await loadPipelineConfig();
  const runtimeEnv = await getRuntimeEnv();
  try {
    await runCommand(
      bqCommand,
      [
        `--project_id=${cfg.projectId}`,
        "query",
        "--nouse_legacy_sql",
        "--max_rows=1",
        "SELECT 1",
      ],
      { env: runtimeEnv, shell: true },
    );
    console.log("[dashboard-api] BigQuery auth check passed.");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] BigQuery auth preflight failed:", detail);
  }
}

async function fetchLiveLeads() {
  const cfg = await loadPipelineConfig();
  const fastSql = `
WITH base AS (
SELECT
  inspection_id,
  account_name,
  region,
  county,
  site_city,
  site_state,
  site_zip,
  distance_from_miramar_miles,
  naics_code,
  industry_segment,
  ownership_type,
  employee_band,
  nr_employees,
  open_case_date,
  close_case_date,
  last_eye_injury_date,
  last_violation_date,
  last_violation_event_date,
  eye_lead_score,
  ppe_score,
  final_score,
  lead_tier,
  pitch_recommendation,
  eye_injury_count,
  fatality_count,
  face_head_injury_count,
  eye_injury_descriptions,
  eye_violation_count,
  prescription_violation_count,
  side_protection_violation_count,
  open_eye_violation_count,
  general_ppe_violation_count,
  open_general_ppe_violation_count,
  willful_violation_count,
  repeat_violation_count,
  total_current_penalty,
  standards_cited,
  violation_event_count,
  contested_violation_count,
  eye_emphasis_count,
  emphasis_code_list,
  related_inspection_count,
  formal_followup_count,
  total_inspection_count,
  has_open_violations
FROM \`${cfg.projectId}.${cfg.dataset}.dashboard_leads_current\`
),
geo_enriched AS (
  SELECT
    b.*,
    ROUND(
      SAFE_DIVIDE(
        ST_DISTANCE(
          zg.internal_point_geom,
          ST_GEOGPOINT(${BAY_AREA_ANCHOR_LON}, ${BAY_AREA_ANCHOR_LAT})
        ),
        1609.344
      ),
      1
    ) AS bay_area_distance_miles,
    ROUND(
      SAFE_DIVIDE(
        ST_DISTANCE(
          zg.internal_point_geom,
          ST_GEOGPOINT(${SAN_DIEGO_ANCHOR_LON}, ${SAN_DIEGO_ANCHOR_LAT})
        ),
        1609.344
      ),
      1
    ) AS san_diego_distance_miles
  FROM base b
  LEFT JOIN \`bigquery-public-data.geo_us_boundaries.zip_codes\` zg
    ON zg.zip_code = LPAD(REGEXP_EXTRACT(COALESCE(b.site_zip, ''), r'(\\d{5})'), 5, '0')
),
eligible_geo AS (
  SELECT
    ge.*,
    (COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}) AS is_within_bay_area_50mi,
    (COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}) AS is_san_diego_area,
    CASE
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
           AND COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
        THEN 'bay_radius|san_diego_area'
      WHEN COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
        THEN 'bay_radius'
      WHEN COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
        THEN 'san_diego_area'
      ELSE 'none'
    END AS geo_match_source
  FROM geo_enriched ge
  WHERE
    UPPER(TRIM(COALESCE(ge.site_state, ''))) = 'CA'
    AND (
      COALESCE(ge.bay_area_distance_miles, 999999) <= ${BAY_AREA_RADIUS_MILES}
      OR COALESCE(ge.san_diego_distance_miles, 999999) <= ${SAN_DIEGO_RADIUS_MILES}
    )
),
classified AS (
  SELECT
    eg.*,
    IFNULL((
      (
        COALESCE(eg.eye_injury_count, 0) > 0
        OR COALESCE(eg.face_head_injury_count, 0) > 0
        OR COALESCE(eg.eye_violation_count, 0) > 0
        OR COALESCE(eg.prescription_violation_count, 0) > 0
        OR COALESCE(eg.open_eye_violation_count, 0) > 0
        OR COALESCE(eg.general_ppe_violation_count, 0) > 0
        OR COALESCE(eg.open_general_ppe_violation_count, 0) > 0
      )
      AND COALESCE(eg.last_eye_injury_date, eg.last_violation_event_date, eg.last_violation_date)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
    ), FALSE) AS qualifies_incident_3yr
  FROM eligible_geo eg
),
all_valid AS (
  -- All geo-eligible companies with a real account name
  SELECT
    *,
    CASE WHEN qualifies_incident_3yr THEN 'incident' ELSE 'profile_fit' END AS lead_type
  FROM classified
  WHERE UPPER(TRIM(COALESCE(account_name, ''))) NOT IN ('', 'NA', 'N/A', 'UNKNOWN', 'NONE', 'NULL')
),
deduped AS (
  -- Keep highest-scoring record per company+zip
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      av.*,
      ROW_NUMBER() OVER (
        PARTITION BY UPPER(COALESCE(av.account_name, '')), COALESCE(av.site_zip, '')
        ORDER BY
          IF(av.qualifies_incident_3yr, 0, 1) ASC,
          av.final_score DESC
      ) AS rn
    FROM all_valid av
  )
  WHERE rn = 1
)
SELECT *
FROM deduped
ORDER BY
  -- Incident leads (3-yr PPE/eye evidence) always rank above profile-fit
  IF(qualifies_incident_3yr, 0, 1) ASC,
  -- Within incident leads: by tier then score
  CASE lead_tier
    WHEN 'P0 Hot Eye'        THEN 0
    WHEN 'P1 Eye Violation'  THEN 1
    WHEN 'P2 PPE Opportunity' THEN 2
    ELSE 3
  END ASC,
  final_score DESC,
  IF(has_open_violations, 1, 0) DESC
LIMIT 600
`;

  const legacySql = `
WITH actionable AS (
  SELECT
    \`Latest Inspection ID\`,
    \`Account Name\`,
    \`Region\`,
    \`Site City\`,
    \`Industry Segment\`,
    \`Ownership Type\`,
    \`Overall Sales Score\`,
    \`Eyewear Evidence Score\`,
    \`Overall Sales Priority\`,
    \`Eyewear Need Tier\`,
    \`Should Look At Now\`,
    \`Matched Sources\`,
    \`Reason To Contact\`,
    \`Why Now\`,
    \`Recent Inspection Context\`,
    \`Has Open Violations\`,
    \`Severe Incident Signal\`,
    \`Direct Prescription Citation Count\`,
    \`Prescription Signal Count\`,
    \`Fit Selection Citation Count\`,
    \`Eye Face Citation Count\`,
    \`General PPE Citation Count\`,
    \`Estimated Employee Band\`
  FROM \`${cfg.projectId}.${cfg.dataset}.eyewear_opportunity_actionable_current\`
),
followup AS (
  SELECT
    \`Latest Inspection ID\`,
    \`Account Name\`,
    \`Region\`,
    \`Case Open Date\`,
    \`Latest Case Close Date\`,
    \`Last Violation Event Date\`,
    \`Last Accident Date\`,
    \`Has Complaint Signal\`,
    \`Standards Cited\`,
    \`Company Latest Load Timestamp\`
  FROM \`${cfg.projectId}.${cfg.dataset}.sales_followup_all_current\`
),
keyed_actionable AS (
  SELECT DISTINCT
    \`Latest Inspection ID\`,
    \`Account Name\`,
    \`Region\`
  FROM actionable
),
followup_latest AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      f.*,
      ROW_NUMBER() OVER (
        PARTITION BY f.\`Latest Inspection ID\`, f.\`Region\`, f.\`Account Name\`
        ORDER BY f.\`Company Latest Load Timestamp\` DESC NULLS LAST
      ) AS rn
    FROM followup f
    INNER JOIN keyed_actionable ka
      ON ka.\`Latest Inspection ID\` = f.\`Latest Inspection ID\`
     AND ka.\`Region\` = f.\`Region\`
     AND ka.\`Account Name\` = f.\`Account Name\`
  )
  WHERE rn = 1
)
SELECT
  *
FROM (
  SELECT
    a.*,
    f.\`Case Open Date\`,
    f.\`Latest Case Close Date\`,
    f.\`Last Violation Event Date\`,
    f.\`Last Accident Date\`,
    f.\`Has Complaint Signal\`,
    f.\`Standards Cited\`,
    f.\`Company Latest Load Timestamp\`
  FROM actionable a
  LEFT JOIN followup_latest f
    ON a.\`Latest Inspection ID\` = f.\`Latest Inspection ID\`
   AND a.\`Region\` = f.\`Region\`
   AND a.\`Account Name\` = f.\`Account Name\`
)
ORDER BY \`Overall Sales Score\` DESC
LIMIT 500
`;
  const runtimeEnv = await getRuntimeEnv();

  const executeSql = async (sqlText) => {
    return runCommand(bqCommand, [
      `--project_id=${cfg.projectId}`,
      "query",
      "--use_legacy_sql=false",
      "--format=prettyjson",
      "--max_rows=600",
    ], { env: runtimeEnv, timeoutMs: 180000, shell: true, input: sqlText });
  };

  const rawJson = await executeSql(fastSql);
  const rows = JSON.parse(rawJson);
  return rows.map(toLeadRecord);
}

async function fetchLiveLeadTableCount() {
  const cfg = await loadPipelineConfig();
  const runtimeEnv = await getRuntimeEnv();
  const countSql = `
SELECT COUNT(*) AS total_rows
FROM \`${cfg.projectId}.${cfg.dataset}.dashboard_leads_current\`
`;

  const rawJson = await runCommand(bqCommand, [
    `--project_id=${cfg.projectId}`,
    "query",
    "--use_legacy_sql=false",
    "--format=prettyjson",
  ], { env: runtimeEnv, timeoutMs: 60000, shell: true, input: countSql });

  const rows = JSON.parse(rawJson);
  const total = Number(rows?.[0]?.total_rows ?? 0);
  return Number.isFinite(total) ? total : 0;
}

async function fetchLiveLeadsCached({ force = false } = {}) {
  const now = Date.now();
  if (!force && leadsCache.generatedAt && now < leadsCache.cacheUntil) {
    return {
      leads: leadsCache.leads,
      generatedAt: leadsCache.generatedAt,
      totalAvailable: leadsCache.totalAvailable,
      cacheHit: true,
    };
  }

  try {
    const [leads, totalAvailable] = await Promise.all([
      fetchLiveLeads(),
      fetchLiveLeadTableCount().catch(() => null),
    ]);
    const generatedAt = new Date().toISOString();
    leadsCache = {
      leads,
      generatedAt,
      totalAvailable,
      cacheUntil: Date.now() + LEADS_CACHE_TTL_MS,
    };
    await writeLeadsSnapshot(leads, generatedAt, totalAvailable);
    return {
      leads,
      generatedAt,
      totalAvailable,
      cacheHit: false,
      stale: false,
    };
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] live lead fetch failed; using stale cache:", detail);
    if (leadsCache.leads.length > 0 && leadsCache.generatedAt) {
      return {
        leads: leadsCache.leads,
        generatedAt: leadsCache.generatedAt,
        totalAvailable: leadsCache.totalAvailable,
        cacheHit: true,
        stale: true,
      };
    }
    throw error;
  }
}

function warmLeadsCacheInBackground() {
  fetchLiveLeadsCached({ force: true }).catch((error) => {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] background lead cache warmup failed:", detail);
  });
}

async function appendHistory(entry) {
  const current = await readHistory();
  current.unshift(entry);
  const sliced = current.slice(0, 40);
  await writeHistory(sliced);
}

app.get("/api/leads", async (req, res) => {
  try {
    const force = String(req.query.force || "").trim() === "1";
    const leadPayload = await fetchLiveLeadsCached({ force });
    const outcomes = await readOutcomes();
    const mergedLeads = leadPayload.leads.map((lead) => {
      const outcome = outcomes[lead.id] || {};
      return {
        ...lead,
        accountStatus: outcome.accountStatus || lead.accountStatus,
        outreachStatus: outcome.outreachStatus || "new",
        outreachNotes: outcome.outreachNotes || "",
        outreachUpdatedAt: outcome.outreachUpdatedAt || "",
      };
    });

    res.json({
      ok: true,
      count: mergedLeads.length,
      totalAvailable: leadPayload.totalAvailable,
      generatedAt: leadPayload.generatedAt,
      cacheHit: leadPayload.cacheHit,
      stale: !!leadPayload.stale,
      leads: mergedLeads,
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : "Failed to load leads",
    });
  }
});

app.get("/api/lead-outcomes", async (_req, res) => {
  const outcomes = await readOutcomes();
  res.json({
    ok: true,
    outcomes,
  });
});

app.post("/api/lead-outcomes", async (req, res) => {
  const leadId = String(req.body?.leadId || "").trim();
  const outreachStatus = String(req.body?.outreachStatus || "").trim();
  const outreachNotes = String(req.body?.outreachNotes || "").trim().slice(0, 2000);

  const allowedStatuses = new Set(["new", "attempted", "connected", "won", "lost"]);
  if (!leadId) {
    res.status(400).json({ ok: false, error: "leadId is required." });
    return;
  }
  if (!allowedStatuses.has(outreachStatus)) {
    res.status(400).json({ ok: false, error: "Invalid outreachStatus." });
    return;
  }

  const outcomes = await readOutcomes();
  const accountStatus =
    outreachStatus === "won"
      ? "Contacted"
      : outreachStatus === "lost" || outreachStatus === "connected"
        ? "In Review"
        : outreachStatus === "attempted"
          ? "Contacted"
          : "New";

  outcomes[leadId] = {
    accountStatus,
    outreachStatus,
    outreachNotes,
    outreachUpdatedAt: new Date().toISOString(),
  };
  await writeOutcomes(outcomes);

  res.json({
    ok: true,
    leadId,
    outcome: outcomes[leadId],
  });
});

app.get("/api/pull-status", (_req, res) => {
  res.json({
    ok: true,
    currentPull,
  });
});

app.get("/api/pull-history", async (_req, res) => {
  const history = await readHistory();
  res.json({
    ok: true,
    history,
  });
});

app.post("/api/refresh-leads", async (_req, res) => {
  if (currentPull?.status === "running") {
    res.status(409).json({
      ok: false,
      error: "A pull is already running.",
      currentPull,
    });
    return;
  }

  const startedAt = new Date().toISOString();
  const pullId = `pull-${Date.now()}`;
  currentPull = {
    id: pullId,
    mode: "refresh",
    status: "running",
    startedAt,
  };

  appendHistory({
    id: pullId,
    status: "running",
    startedAt,
    endedAt: null,
    durationSeconds: null,
    message: "Pull started.",
  }).catch(() => {});

  res.json({
    ok: true,
    pull: currentPull,
  });

  const startMs = Date.now();
  try {
    const runtimeEnv = await getRuntimeEnv();
    const pythonCommand = await resolvePythonCommand();
    await runCommand(pythonCommand, ["-m", "pipeline.cli", "refresh-leads-v3"], {
      cwd: repoRoot,
      env: runtimeEnv,
      timeoutMs: 180000,
    });

    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    currentPull = {
      id: pullId,
      mode: "refresh",
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
    };

    warmLeadsCacheInBackground();

    await appendHistory({
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
      message: "Sales priority refresh completed.",
    });
  } catch (error) {
    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    const detail = error instanceof Error ? error.message : "Pipeline pull failed";

    currentPull = {
      id: pullId,
      mode: "refresh",
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      error: detail,
    };

    await appendHistory({
      id: pullId,
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      message: detail,
    });
  }
});

app.post("/api/full-pipeline", async (_req, res) => {
  if (currentPull?.status === "running") {
    res.status(409).json({
      ok: false,
      error: "A pull is already running.",
      currentPull,
    });
    return;
  }

  const startedAt = new Date().toISOString();
  const pullId = `pull-${Date.now()}`;
  currentPull = {
    id: pullId,
    mode: "full",
    status: "running",
    startedAt,
  };

  appendHistory({
    id: pullId,
    status: "running",
    startedAt,
    endedAt: null,
    durationSeconds: null,
    message: "Full pipeline started.",
  }).catch(() => {});

  res.json({
    ok: true,
    pull: currentPull,
  });

  const startMs = Date.now();
  try {
    const runtimeEnv = await getRuntimeEnv();
    const pythonCommand = await resolvePythonCommand();
    await runCommand(pythonCommand, ["-m", "pipeline.cli", "run-full"], {
      cwd: repoRoot,
      env: runtimeEnv,
      timeoutMs: 900000,
    });

    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    currentPull = {
      id: pullId,
      mode: "full",
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
    };

    warmLeadsCacheInBackground();

    await appendHistory({
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
      message: "Full pipeline run completed.",
    });
  } catch (error) {
    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    const detail = error instanceof Error ? error.message : "Full pipeline failed";

    currentPull = {
      id: pullId,
      mode: "full",
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      error: detail,
    };

    await appendHistory({
      id: pullId,
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      message: detail,
    });
  }
});

app.listen(port, () => {
  console.log(`[dashboard-api] running on http://127.0.0.1:${port}`);
  ensureBigQueryAuth().catch(() => {});
  readLeadsSnapshot()
    .then((snapshot) => {
      if (!snapshot) {
        return;
      }
      leadsCache = {
        leads: snapshot.leads,
        generatedAt: snapshot.generatedAt,
        totalAvailable: snapshot.totalAvailable,
        cacheUntil: Date.now() + LEADS_CACHE_TTL_MS,
      };
    })
    .finally(() => {
      warmLeadsCacheInBackground();
    });
});
