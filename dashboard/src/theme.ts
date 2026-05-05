import { createTheme } from "@mui/material/styles";

export type DashboardTheme = "signal" | "neutral";

export function buildTheme(mode: DashboardTheme) {
  const signal = mode === "signal";

  return createTheme({
    palette: {
      mode: "light",
      primary: {
        main: signal ? "#12324a" : "#345c7c",
      },
      secondary: {
        main: signal ? "#c7773f" : "#6b7280",
      },
      background: {
        default: signal ? "#f7f2e9" : "#f3f5f7",
        paper: "#fffaf3",
      },
      success: {
        main: "#2c7a5a",
      },
      warning: {
        main: "#be7a22",
      },
      error: {
        main: "#b84a3a",
      },
      text: {
        primary: "#1b2a36",
        secondary: "#5b6670",
      },
    },
    shape: {
      borderRadius: 18,
    },
    typography: {
      fontFamily: '"Montserrat", "Segoe UI", "Helvetica Neue", sans-serif',
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
            border: "1px solid rgba(18, 50, 74, 0.12)",
            boxShadow: "0 14px 30px rgba(18, 50, 74, 0.09)",
          },
        },
      },
      MuiDrawer: {
        styleOverrides: {
          paper: {
            background:
              "linear-gradient(180deg, rgba(15,43,64,0.98) 0%, rgba(24,58,84,0.98) 100%)",
            color: "#f6f3ee",
          },
        },
      },
    },
  });
}
