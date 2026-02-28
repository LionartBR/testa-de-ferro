// TreemapContratos — proportional block grid for contract value visualization.
//
// Design decision: Flex-wrap layout instead of a true treemap algorithm (e.g.
// Squarify). A proper treemap requires a recursive area-subdivision algorithm
// that is non-trivial to implement correctly in pure CSS. The flex-wrap
// approach gives an honest proportional area approximation that is sufficient
// for this use case (relative comparison between contracts) without adding a
// charting library dependency.
//
// Block size is computed as a percentage of the container width using the
// contract's share of the total value. A minimum size of 80px guarantees
// legibility for small-value contracts. The `padding-top` trick maintains
// a square aspect ratio per block.
//
// Color palette: a fixed set of muted blues/greens to distinguish blocks
// without implying semantic meaning (color here encodes identity, not risk).

import type { ContratoResumo } from "@/types/contrato";
import { formatCurrency } from "@/lib/formatters";

interface TreemapContratosProps {
  contratos: ContratoResumo[];
}

// ADR: Fixed palette of 8 colors cycling by index. Avoids dynamic color
// generation (HSL rotation) which can produce illegible low-contrast colors.
const BLOCK_COLORS: readonly string[] = [
  "#3b82f6", // blue-500
  "#06b6d4", // cyan-500
  "#10b981", // emerald-500
  "#6366f1", // indigo-500
  "#8b5cf6", // violet-500
  "#0ea5e9", // sky-500
  "#14b8a6", // teal-500
  "#60a5fa", // blue-400
] as const;

const MIN_BLOCK_PX = 80;

export function TreemapContratos({ contratos }: TreemapContratosProps) {
  if (contratos.length === 0) {
    return (
      <p className="text-sm text-gray-500">Nenhum contrato para exibir.</p>
    );
  }

  const values = contratos.map((c) => {
    const parsed = parseFloat(c.valor);
    return isNaN(parsed) ? 0 : parsed;
  });

  const total = values.reduce((sum, v) => sum + v, 0);

  return (
    <div className="flex flex-wrap gap-1" role="list" aria-label="Treemap de contratos por valor">
      {contratos.map((contrato, index) => {
        const value = values[index] ?? 0;
        // Share of total expressed as percentage (0–100). When total is zero
        // all blocks get equal space to avoid division-by-zero rendering.
        const share = total > 0 ? (value / total) * 100 : 100 / contratos.length;
        const color = BLOCK_COLORS[index % BLOCK_COLORS.length];

        return (
          <div
            key={index}
            role="listitem"
            title={`${contrato.orgao_codigo} — ${formatCurrency(contrato.valor)}`}
            style={{
              // flex-basis drives width; the padding-top trick makes height
              // equal to width, producing a square block whose area is
              // proportional to share².  For a simple area-proportional layout
              // we set both dimensions to sqrt(share) * scale_factor.
              //
              // We use inline min() via calc to enforce MIN_BLOCK_PX floor.
              flexBasis: `calc(max(${MIN_BLOCK_PX}px, ${Math.sqrt(share) * 9}%))`,
              paddingTop: `calc(max(${MIN_BLOCK_PX}px, ${Math.sqrt(share) * 9}%))`,
              backgroundColor: color,
              position: "relative",
            }}
            className="overflow-hidden rounded"
          >
            {/* Absolute overlay so text sits inside the padding-top square */}
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-0.5 p-1 text-center"
            >
              <span className="line-clamp-1 text-xs font-semibold text-white drop-shadow">
                {contrato.orgao_codigo}
              </span>
              <span className="text-xs text-white/90 drop-shadow">
                {formatCurrency(contrato.valor)}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}
