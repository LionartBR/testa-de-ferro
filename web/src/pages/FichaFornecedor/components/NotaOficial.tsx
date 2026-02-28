// NotaOficial — placeholder for a company's official response / contestation.
//
// Design decisions:
// - The backend does not yet serve official notes. This component renders a
//   neutral informational state explaining the feature rather than rendering
//   nothing or crashing, so the page layout remains complete.
// - notaUrl prop is kept for future use when the backend supports it.
//   When null, the empty/placeholder state is shown.
// - External link opens in new tab with rel="noopener noreferrer" per security
//   best practice (no Referer leak to the third-party destination).

import { Card, CardHeader } from "@/components/ui/Card";

interface NotaOficialProps {
  notaUrl: string | null;
}

export function NotaOficial({ notaUrl }: NotaOficialProps) {
  return (
    <Card>
      <CardHeader title="Nota Oficial da Empresa" />

      {notaUrl ? (
        <a
          href={notaUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-sm font-medium text-blue-600 hover:underline"
        >
          Acessar nota oficial
          <span aria-hidden="true">↗</span>
        </a>
      ) : (
        <p className="text-sm text-gray-500">
          Esta empresa não apresentou nota oficial ou contestação até o momento.
          Empresas identificadas podem encaminhar resposta formal através do
          canal de contato indicado na página de metodologia.
        </p>
      )}
    </Card>
  );
}
