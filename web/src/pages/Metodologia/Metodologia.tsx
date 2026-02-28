import { PageContainer } from "@/components/layout/PageContainer";
import { Card } from "@/components/ui/Card";
import { ExplicacaoIndicadores } from "./components/ExplicacaoIndicadores";
import { ExplicacaoAlertas } from "./components/ExplicacaoAlertas";
import { FontesDados } from "./components/FontesDados";
import { Limitacoes } from "./components/Limitacoes";
import { Changelog } from "./components/Changelog";

interface SectionProps {
  id: string;
  title: string;
  description?: string;
  children: React.ReactNode;
}

function Section({ id, title, description, children }: SectionProps) {
  return (
    <section id={id}>
      <div className="mb-4">
        <h2 className="text-lg font-bold text-gray-900">{title}</h2>
        {description && (
          <p className="mt-1 text-sm text-gray-500">{description}</p>
        )}
      </div>
      <Card className="overflow-hidden p-0">
        <div className="p-4">{children}</div>
      </Card>
    </section>
  );
}

export function Metodologia() {
  return (
    <PageContainer>
      <div className="space-y-10">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Metodologia</h1>
          <p className="mt-2 max-w-2xl text-sm text-gray-600">
            Esta página descreve como o Testa de Ferro detecta empresas com
            perfil suspeito. Todos os critérios são públicos, auditáveis e
            baseados exclusivamente em dados abertos do governo federal.
          </p>
        </div>

        <div className="rounded-md border border-blue-200 bg-blue-50 px-4 py-3">
          <p className="text-sm font-medium text-blue-900">
            Sistema Dual: Alertas e Score são dimensões independentes
          </p>
          <p className="mt-1 text-sm text-blue-700">
            Um fornecedor pode ter score zero e alertas gravíssimos — ou score
            alto e nenhum alerta. As duas dimensões nunca se alimentam
            mutuamente. Alertas são flags binárias; o score é uma soma ponderada
            de indicadores fracos que, combinados, revelam padrão.
          </p>
        </div>

        <Section
          id="indicadores"
          title="Indicadores Cumulativos (Score)"
          description="Cada indicador tem um peso. O score é a soma dos pesos dos indicadores presentes. Indicadores isolados têm baixo poder preditivo — é a combinação que revela o padrão."
        >
          <ExplicacaoIndicadores />
        </Section>

        <Section
          id="alertas"
          title="Alertas Críticos"
          description="Alertas são binários: ou a condição está presente, ou não está. Não há escala. A severidade (Grave ou Gravíssimo) indica a urgência da revisão manual."
        >
          <ExplicacaoAlertas />
        </Section>

        <Section
          id="fontes"
          title="Fontes de Dados"
          description="Todos os dados são públicos e de origem governamental. O pipeline é executado periodicamente e substituído atomicamente — nunca dados parciais são servidos."
        >
          <FontesDados />
        </Section>

        <Section
          id="limitacoes"
          title="Limitações Conhecidas"
          description="Transparência sobre o que o sistema não captura é parte essencial da metodologia. Leia antes de usar os dados para qualquer análise."
        >
          <Limitacoes />
        </Section>

        <Section
          id="changelog"
          title="Histórico de Mudanças nos Critérios"
          description="Alterações nos pesos, thresholds ou regras de detecção são registradas aqui para rastreabilidade."
        >
          <Changelog />
        </Section>
      </div>
    </PageContainer>
  );
}
