import type { OshaViolationDetail } from "./types";

type OshaCodeMap = {
  code: string;
  title: string;
  plainEnglish: string;
  source: OshaViolationDetail["source"];
};

const OSHA_CODE_MAP: OshaCodeMap[] = [
  {
    code: "1910.133",
    title: "Eye and face protection",
    plainEnglish: "Workers need proper eye and face protection whenever the job can hurt their eyes or face.",
    source: "OSHA 1910",
  },
  {
    code: "1910.133(a)(2)",
    title: "Side protection for flying objects",
    plainEnglish: "Eye protection must also shield the sides when debris or flying objects are a hazard.",
    source: "OSHA 1910",
  },
  {
    code: "1910.133(a)(3)",
    title: "Prescription lens protection",
    plainEnglish: "If workers wear prescription lenses, the safety eyewear has to work safely with those lenses.",
    source: "OSHA 1910",
  },
  {
    code: "1910.132",
    title: "General PPE requirements",
    plainEnglish: "Employers must provide and maintain protective equipment when job hazards can injure workers.",
    source: "OSHA 1910",
  },
  {
    code: "1910.132(d)(1)",
    title: "Hazard assessment and PPE selection",
    plainEnglish: "The employer must evaluate the workplace hazards and choose PPE that actually protects workers from them.",
    source: "OSHA 1910",
  },
  {
    code: "1910.132(f)",
    title: "PPE training",
    plainEnglish: "Workers must be trained on when PPE is needed, what to use, and how to wear it correctly.",
    source: "OSHA 1910",
  },
  {
    code: "1926.102",
    title: "Construction eye and face protection",
    plainEnglish: "Construction workers need appropriate eye and face protection when the job creates eye hazards.",
    source: "OSHA 1926",
  },
  {
    code: "1926.102(a)(2)",
    title: "Construction side protection",
    plainEnglish: "Construction eye protection must include side protection when objects can fly toward the worker.",
    source: "OSHA 1926",
  },
  {
    code: "1926.102(a)(3)",
    title: "Construction prescription lens protection",
    plainEnglish: "Construction workers with prescription glasses need eye protection designed to work with them safely.",
    source: "OSHA 1926",
  },
  {
    code: "1926.95",
    title: "Construction PPE criteria",
    plainEnglish: "Construction employers must provide and maintain protective equipment when hazards make it necessary.",
    source: "OSHA 1926",
  },
  {
    code: "1926.95(c)(2)",
    title: "Proper PPE fit",
    plainEnglish: "The PPE chosen for each worker has to fit properly to protect them well.",
    source: "OSHA 1926",
  },
];

function normalizeCode(code: string) {
  return code.trim().toLowerCase();
}

function fallbackDetail(code: string): OshaViolationDetail {
  return {
    code,
    title: "OSHA standard reference",
    plainEnglish: "This citation points to an OSHA safety requirement that should be reviewed with the exact inspection record.",
    source: code.startsWith("1926.") ? "OSHA 1926" : code.startsWith("1910.") ? "OSHA 1910" : "OSHA General",
  };
}

export function toViolationDetails(codes: string[]): OshaViolationDetail[] {
  return codes.map((code) => {
    const exact = OSHA_CODE_MAP.find((entry) => normalizeCode(entry.code) === normalizeCode(code));
    if (exact) {
      return exact;
    }

    const parent = OSHA_CODE_MAP.find((entry) => normalizeCode(code).startsWith(normalizeCode(entry.code)));
    return parent ?? fallbackDetail(code);
  });
}
