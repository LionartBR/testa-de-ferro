# Frontend Architect Memory — Testa de Ferro

## Critical TypeScript Pattern

`ApiState<T>` is a discriminated union — NEVER use `interface X extends ApiState<T>`.
Use `type X = ApiState<T> & { extra: fields }` instead.
See: `src/hooks/useApi.ts`, `src/pages/Ranking/hooks/useRanking.ts`.

## useApi Contract

- Pass `null` to stay in "idle" state (used in Busca when query is too short).
- Re-fetches when fetcher reference changes — always wrap fetcher in `useCallback`.
- Returns `{status, data, error, refetch}` spread from `ApiState<T>`.

## Table<T> Render Signature

`render: (item: T) => React.ReactNode` — no index parameter.
For positional columns, pre-map data to `{position, item}` before passing to Table.

## Project Conventions

- Named exports for all page components: `export function Ranking()`.
- All pages wrapped in `<PageContainer>`.
- `hasMore` = `data.length === limit`.
- Client-side filtering layered on top of server-side tipo filtering (Alertas page).
- URL sync via `useSearchParams` in Busca page.
- `noUncheckedIndexedAccess: true` — never access array indices without guard.

## UI Component Contracts (confirmed)
- `Card` + `CardHeader` from `@/components/ui/Card` — CardHeader: `title`, `subtitle?`, `action?`
- `Table<T>` from `@/components/ui/Table` — generic: `columns`, `data`, `keyExtractor`
- `Button` variants: primary/secondary/ghost; sizes: sm/md
- `Badge` — className controls color; no variant prop
- `ValorMonetario` — props: `{ valor: string | number, className? }`
- `CNPJFormatado` — props: `{ cnpj, link?: boolean }` (link defaults true)

## FichaFornecedor Architecture
- `score: Score | null` and `endereco: Endereco | null` — always guard
- `useParams` returns `string | undefined` — guard before passing to hooks
- `useApi` fetcher must be stabilized with `useCallback(fn, [cnpj])`
- Alert grouping: pure transformation at render time via useMemo (no state)
- `<details>/<summary>` for expandable groups — native, zero JS state
- Two-column layout: `lg:grid-cols-3`, left=2cols (analysis), right=1col (context)

## Testing Patterns
- Mock at service module: `vi.mock("@/services/fornecedorService", () => ({...}))`
- Import mocked module AFTER vi.mock() to get the mocked version
- `findByRole("heading", { level: 1 })` when text appears in multiple places
- `details` elements = `role="group"` in jsdom
- `role="note"` targets DisclaimerBanner
- Fixture disclaimer text uses "automatizados" (not "automáticos")
