// ScoreGauge — SVG semicircular gauge for risk score visualization.
//
// Design decision: Pure SVG, no external chart library. The semicircle spans
// 180 degrees (left to right). The arc is built with SVG path commands using
// a fixed viewBox so the component scales freely with its container.
//
// The needle-less approach (filled arc) was chosen over a needle gauge because
// it reads faster at a glance and degrades gracefully at small sizes.
//
// Color is driven entirely by faixa, not by valor interpolation, which keeps
// the mapping consistent with the rest of the UI (ScoreBadge, etc.).

import type { FaixaRisco } from "@/types/score";
import { FAIXA_BG } from "@/lib/colors";

interface ScoreGaugeProps {
  valor: number;
  faixa: FaixaRisco;
}

// viewBox geometry constants — all coordinates are in this coordinate space.
const CX = 100;
const CY = 100;
const RADIUS_OUTER = 80;
const RADIUS_INNER = 54;
const START_ANGLE_DEG = 180; // leftmost point of the semicircle
const TOTAL_ARC_DEG = 180;

function degreesToRadians(deg: number): number {
  return (deg * Math.PI) / 180;
}

function polarToCartesian(
  cx: number,
  cy: number,
  radius: number,
  angleDeg: number,
): { x: number; y: number } {
  const rad = degreesToRadians(angleDeg);
  return {
    x: cx + radius * Math.cos(rad),
    y: cy + radius * Math.sin(rad),
  };
}

// Builds an SVG arc path for a ring segment (donut slice).
// Angles follow SVG conventions: 0° = right, 90° = down, 180° = left.
function buildArcPath(
  cx: number,
  cy: number,
  outerRadius: number,
  innerRadius: number,
  startDeg: number,
  endDeg: number,
): string {
  const outerStart = polarToCartesian(cx, cy, outerRadius, startDeg);
  const outerEnd = polarToCartesian(cx, cy, outerRadius, endDeg);
  const innerStart = polarToCartesian(cx, cy, innerRadius, endDeg);
  const innerEnd = polarToCartesian(cx, cy, innerRadius, startDeg);

  const largeArc = endDeg - startDeg > 180 ? 1 : 0;

  return [
    `M ${outerStart.x} ${outerStart.y}`,
    `A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${outerEnd.x} ${outerEnd.y}`,
    `L ${innerStart.x} ${innerStart.y}`,
    `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${innerEnd.x} ${innerEnd.y}`,
    "Z",
  ].join(" ");
}

export function ScoreGauge({ valor, faixa }: ScoreGaugeProps) {
  const clampedValor = Math.max(0, Math.min(100, valor));

  // Map valor (0–100) to arc degrees within the semicircle (180°–360°).
  // At valor=0 the filled arc has zero length; at valor=100 it fills the
  // entire semicircle.
  const fillDeg = (clampedValor / 100) * TOTAL_ARC_DEG;
  const fillEndAngle = START_ANGLE_DEG + fillDeg;

  const trackPath = buildArcPath(
    CX,
    CY,
    RADIUS_OUTER,
    RADIUS_INNER,
    START_ANGLE_DEG,
    START_ANGLE_DEG + TOTAL_ARC_DEG,
  );

  const fillPath =
    fillDeg > 0
      ? buildArcPath(
          CX,
          CY,
          RADIUS_OUTER,
          RADIUS_INNER,
          START_ANGLE_DEG,
          fillEndAngle,
        )
      : null;

  const fillColor = FAIXA_BG[faixa];

  return (
    // viewBox="0 50 200 110" clips the bottom half of the 200×200 coordinate
    // space, giving us only the semicircle without wasted vertical space.
    <svg
      viewBox="0 50 200 110"
      aria-label={`Score de risco: ${clampedValor} — ${faixa}`}
      role="img"
    >
      {/* Background track */}
      <path d={trackPath} fill="#e5e7eb" />

      {/* Filled arc representing the score value */}
      {fillPath !== null && <path d={fillPath} fill={fillColor} />}

      {/* Numeric value centered in the gauge opening */}
      <text
        x={CX}
        y={CY + 10}
        textAnchor="middle"
        dominantBaseline="middle"
        fontSize="28"
        fontWeight="700"
        fill={fillColor}
      >
        {clampedValor}
      </text>

      {/* Faixa label below the numeric value */}
      <text
        x={CX}
        y={CY + 34}
        textAnchor="middle"
        dominantBaseline="middle"
        fontSize="11"
        fill="#6b7280"
      >
        {faixa}
      </text>

      {/* Scale labels at 0 and 100 at the ends of the semicircle */}
      <text x="14" y="108" textAnchor="middle" fontSize="9" fill="#9ca3af">
        0
      </text>
      <text x="186" y="108" textAnchor="middle" fontSize="9" fill="#9ca3af">
        100
      </text>
    </svg>
  );
}
