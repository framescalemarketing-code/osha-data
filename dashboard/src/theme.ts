import { createTheme } from "@mui/material/styles";

export type DashboardTheme = "signal" | "neutral";

export function buildTheme(mode: DashboardTheme) {
  const signal = mode === "signal";

  return createTheme({
    palette: {
      mode: "light",
      primary: {
        main: signal ? "#1f6f78" : "#345c7c",
      },
      secondary: {
        main: signal ? "#c96f31" : "#6b7280",
      },
      background: {
        default: signal ? "#f4efe7" : "#f3f5f7",
        paper: "#fffdf9",
      },
      success: {
        main: "#2e7d5b",
      },
      warning: {
        main: "#c57c1f",
      },
      error: {
        main: "#bd4d3e",
      },
      text: {
        primary: "#1e2932",
        secondary: "#56616b",
      },
    },
    shape: {
      borderRadius: 18,
    },
    typography: {
      fontFamily: '"Segoe UI", "Helvetica Neue", sans-serif',
      h3: {
        fontWeight: 700,
      },
      h4: {
        fontWeight: 700,
      },
      h5: {
        fontWeight: 700,
      },
      h6: {
        fontWeight: 700,
      },
      button: {
        textTransform: "none",
        fontWeight: 700,
      },
    },
    components: {
      MuiCard: {
        styleOverrides: {
          root: {
            border: "1px solid rgba(31, 41, 55, 0.08)",
            boxShadow: "0 18px 40px rgba(27, 39, 51, 0.08)",
          },
        },
      },
      MuiDrawer: {
        styleOverrides: {
          paper: {
            background:
              "linear-gradient(180deg, rgba(28,60,73,0.98) 0%, rgba(42,47,66,0.98) 100%)",
            color: "#f6f3ee",
          },
        },
      },
    },
  });
}
