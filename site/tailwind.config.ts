import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        accent: {
          50: "#f0fdfa",
          100: "#ccfbf1",
          500: "#14b8a6",
          600: "#0d9488",
          700: "#0f766e",
          800: "#115e59",
        },
        warm: {
          50: "#fffbeb",
          100: "#fef3c7",
          200: "#fde68a",
          500: "#f59e0b",
          600: "#d97706",
          700: "#b45309",
        },
        ink: {
          900: "#111827",
          700: "#374151",
          500: "#6b7280",
          300: "#d1d5db",
          100: "#f3f4f6",
          50: "#f9fafb",
        },
      },
      backgroundImage: {
        "paw-pattern":
          "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='90' height='90' viewBox='0 0 90 90'><g fill='none' stroke='%23d97706' stroke-width='1.4' opacity='0.14'><circle cx='20' cy='30' r='4'/><circle cx='32' cy='22' r='3.5'/><circle cx='44' cy='22' r='3.5'/><circle cx='56' cy='30' r='4'/><ellipse cx='38' cy='42' rx='10' ry='8'/><circle cx='64' cy='68' r='3.5'/><circle cx='72' cy='60' r='3'/><circle cx='80' cy='60' r='3'/><ellipse cx='75' cy='72' rx='7' ry='5.5'/></g></svg>\")",
      },
      fontFamily: {
        sans: [
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "Helvetica",
          "Arial",
          "sans-serif",
        ],
      },
    },
  },
  plugins: [],
};

export default config;
