# Frontend Architect — Agent Memory

## Project: Testa de Ferro

Stack: React 18 + TypeScript strict + Tailwind + Vite + Vitest.
Working directory: `C:/studying/testa-de-ferro`.

## Key Patterns

**Named exports always.** Every component uses `export function ComponentName`, never default exports.

**Import aliases.** `@/` maps to `web/src/`. Use `@/types/...`, `@/lib/...`, `@/components/...`.

**Type sources.**
- `FaixaRisco` and `Score` → `@/types/score`
- `ContratoResumo` → `@/types/contrato`
- Color maps → `@/lib/colors` (`FAIXA_BG` for hex, `FAIXA_COLORS` for Tailwind classes)
- Formatters → `@/lib/formatters` (`formatCurrency`, `formatDate`, `formatCNPJ`, `formatNumber`)

**No chart libraries.** SVG and Tailwind-only for all chart components confirmed in ScoreGauge, ContratosTimeline, TreemapContratos.

**SVG gauge pattern.** viewBox clipped to show only the visible region (e.g. `"0 50 200 110"` for a semicircle). Arc paths built with `polarToCartesian` + `buildArcPath` helpers. `degreesToRadians` inline, no Math lib.

**Proportional block layout.** For treemap-style visuals: `flex-wrap` + `flex-basis` as `calc(max(MIN_px, sqrt(share)% * scale))`. `padding-top` trick for square aspect ratio. Absolute overlay for text inside the square.

**Tailwind vertical timeline.** `border-l border-gray-200` as spine, `absolute -left-2` for dot, `ml-6` for content offset.

**Test runner.** `npm run test -- --run` (non-interactive). `npm run typecheck` for `tsc --noEmit`.

## File Size Limits (CLAUDE.md)

200–400 lines per file in this project (smaller than the Hugo rule of 600–800).

## Test Infrastructure

- **NOT installed:** `@testing-library/user-event` — use `fireEvent` + `act` from `@testing-library/react`
- **vi.mock() BEFORE imports**, import mocked modules AFTER mock declaration
- **Timer control:** `vi.useFakeTimers()` + `act(() => { vi.advanceTimersByTime(N); })` for debounce; `vi.useRealTimers()` in `afterEach`
- **Loading spinner:** `.animate-spin` CSS class on the spinner div

## Hook → Service Mapping

| Hook | Service call |
|------|-------------|
| `useAlertasFeed` | `alertaService.getAlertas` |
| `useStats` | `statsService.getStats` |
| `useRanking` | `fornecedorService.getRanking` |
| `useBusca` | `buscaService.buscar` (debounced 300ms, min 2 chars) |
| `useAlertas(tipo)` | `alertaService.getAlertas` or `getAlertasPorTipo` |
| `useGrafo` | `fornecedorService.getGrafo` (route param `:cnpj`) |
| `useDashboardOrgao` | `orgaoService.getDashboard` (route param `:codigo`) |

## Common Test Pitfalls

- "Empresa" text appears in GrafoLegenda AND NoTooltip → use `findByText("Score de risco")` after click
- "FORNECEDOR TOP" appears in TopFornecedores table AND ContratosChart → use `findAllByText`
- `AlertaFiltros` labels: `getByLabelText("Tipo")` and `getByLabelText("Severidade")` via `htmlFor`/`id`
- `GrafoControles` checkboxes: `getByRole("checkbox", { name: "Sócios" })` works via label wrapping
- `Pagination` buttons: labeled "Anterior" and "Próxima" (HTML entities in JSX render as plain text)
