// SecaoScore — score badge + breakdown of every active indicator.
//
// Design decisions:
// - Score can be null (supplier never scored). In that case a neutral empty
//   state is shown instead of hiding the section — maintaining layout
//   consistency across all ficha pages.
// - Each Indicador row shows peso as a numeric pill so analysts can quickly
//   reason about which indicators are driving the total.
// - The evidencia field gives the raw data behind the indicator — shown in
//   muted monospace to visually separate evidence from interpretation.

import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { ScoreBadge } from "@/components/ScoreBadge";
import { INDICADOR_LABELS } from "@/lib/constants";
import type { Score } from "@/types/score";

interface IndicadorRowProps {
  peso: number;
  label: string;
  descricao: string;
  evidencia: string;
}

function IndicadorRow({ peso, label, descricao, evidencia }: IndicadorRowProps) {
  return (
    <div className="flex items-start gap-3 rounded-md bg-gray-50 px-3 py-2.5">
      <span
        title="Peso do indicador no score"
        className="mt-0.5 w-8 shrink-0 rounded bg-gray-200 px-1.5 py-0.5 text-center text-xs font-semibold tabular-nums text-gray-700"
      >
        +{peso}
      </span>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-gray-900">{label}</p>
        <p className="text-sm text-gray-600">{descricao}</p>
        {evidencia && (
          <p className="mt-0.5 font-mono text-xs text-gray-400">{evidencia}</p>
        )}
      </div>
    </div>
  );
}

interface SecaoScoreProps {
  score: Score | null;
}

export function SecaoScore({ score }: SecaoScoreProps) {
  const action = score ? (
    <ScoreBadge valor={score.valor} faixa={score.faixa} />
  ) : null;

  return (
    <Card>
      <CardHeader title="Score de Risco" action={action} />

      {!score || score.indicadores.length === 0 ? (
        <EmptyState message="Nenhum indicador cumulativo identificado." />
      ) : (
        <div className="space-y-2">
          {score.indicadores.map((indicador, index) => (
            <IndicadorRow
              key={index}
              peso={indicador.peso}
              label={INDICADOR_LABELS[indicador.tipo]}
              descricao={indicador.descricao}
              evidencia={indicador.evidencia}
            />
          ))}
        </div>
      )}
    </Card>
  );
}
