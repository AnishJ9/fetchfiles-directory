import { ImageResponse } from "next/og";
import { countByMetro } from "@/lib/data";

export const alt = "Fetch Directory — Find your pet's people";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default async function Image() {
  const counts = countByMetro();
  const totalListings = Object.values(counts).reduce((a, b) => a + b, 0);

  // Warm cream bg + scattered paw prints + amber circle mark + headline
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          background:
            "linear-gradient(135deg, #fffbeb 0%, #fef3c7 55%, #fde68a 100%)",
          display: "flex",
          flexDirection: "column",
          padding: "72px 80px",
          position: "relative",
          fontFamily: "system-ui, -apple-system, sans-serif",
        }}
      >
        {/* Decorative paw prints */}
        <PawCluster
          style={{
            position: "absolute",
            top: 40,
            right: 60,
            opacity: 0.18,
            transform: "rotate(15deg)",
          }}
          scale={1.4}
        />
        <PawCluster
          style={{
            position: "absolute",
            bottom: 60,
            right: 260,
            opacity: 0.13,
            transform: "rotate(-8deg)",
          }}
          scale={1.0}
        />
        <PawCluster
          style={{
            position: "absolute",
            top: 280,
            left: 720,
            opacity: 0.10,
            transform: "rotate(35deg)",
          }}
          scale={0.8}
        />

        {/* Logo mark + wordmark */}
        <div style={{ display: "flex", alignItems: "center", gap: 18 }}>
          <div
            style={{
              width: 72,
              height: 72,
              borderRadius: 72,
              background: "#fef3c7",
              border: "3px solid #d97706",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <PawCluster scale={0.7} color="#b45309" opacity={1} />
          </div>
          <div
            style={{
              fontSize: 38,
              fontWeight: 600,
              color: "#111827",
              letterSpacing: -0.5,
            }}
          >
            Fetch Directory
          </div>
        </div>

        {/* Headline */}
        <div
          style={{
            marginTop: 80,
            fontSize: 100,
            fontWeight: 700,
            color: "#111827",
            letterSpacing: -2,
            lineHeight: 1.05,
            display: "flex",
          }}
        >
          Find your pet&apos;s
        </div>
        <div
          style={{
            fontSize: 100,
            fontWeight: 700,
            color: "#b45309",
            letterSpacing: -2,
            lineHeight: 1.05,
            display: "flex",
          }}
        >
          people.
        </div>

        {/* Subhead */}
        <div
          style={{
            marginTop: 28,
            fontSize: 28,
            color: "#374151",
            maxWidth: 820,
            lineHeight: 1.35,
            display: "flex",
          }}
        >
          Vets, groomers, boarders, daycares, and sitters across five metros.
        </div>

        {/* Footer row: stats */}
        <div
          style={{
            position: "absolute",
            bottom: 56,
            left: 80,
            right: 80,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 20,
              fontSize: 22,
              color: "#6b7280",
            }}
          >
            <span style={{ color: "#111827", fontWeight: 600 }}>
              {totalListings.toLocaleString()} listings
            </span>
            <span>·</span>
            <span>Atlanta</span>
            <span>·</span>
            <span>Austin</span>
            <span>·</span>
            <span>Tampa</span>
            <span>·</span>
            <span>Nashville</span>
            <span>·</span>
            <span>Asheville</span>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              fontSize: 20,
              color: "#0f766e",
              fontWeight: 600,
            }}
          >
            fetch-directory
          </div>
        </div>
      </div>
    ),
    size,
  );
}

// A simple paw-print shape built from divs (next/og doesn't support arbitrary SVG well)
function PawCluster({
  scale = 1,
  color = "#d97706",
  opacity = 1,
  style,
}: {
  scale?: number;
  color?: string;
  opacity?: number;
  style?: React.CSSProperties;
}) {
  const s = (n: number) => n * scale;
  return (
    <div
      style={{
        width: s(120),
        height: s(120),
        position: "relative",
        display: "flex",
        opacity,
        ...style,
      }}
    >
      {/* four toe pads */}
      <div
        style={{
          position: "absolute",
          top: s(18),
          left: s(10),
          width: s(22),
          height: s(26),
          background: color,
          borderRadius: s(22),
        }}
      />
      <div
        style={{
          position: "absolute",
          top: s(4),
          left: s(40),
          width: s(22),
          height: s(26),
          background: color,
          borderRadius: s(22),
        }}
      />
      <div
        style={{
          position: "absolute",
          top: s(4),
          left: s(72),
          width: s(22),
          height: s(26),
          background: color,
          borderRadius: s(22),
        }}
      />
      <div
        style={{
          position: "absolute",
          top: s(18),
          left: s(95),
          width: s(22),
          height: s(26),
          background: color,
          borderRadius: s(22),
        }}
      />
      {/* main paw pad */}
      <div
        style={{
          position: "absolute",
          top: s(55),
          left: s(28),
          width: s(70),
          height: s(56),
          background: color,
          borderRadius: s(40),
        }}
      />
    </div>
  );
}
