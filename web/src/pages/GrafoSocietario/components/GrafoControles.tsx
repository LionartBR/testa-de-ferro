import { Button } from "@/components/ui/Button";

interface GrafoControlesProps {
  showEmpresas: boolean;
  showSocios: boolean;
  onToggleEmpresas: () => void;
  onToggleSocios: () => void;
  onReset: () => void;
}

// Placeholder controls for graph filtering.
// Will be extended with zoom controls once the force-directed lib is added.
export function GrafoControles({
  showEmpresas,
  showSocios,
  onToggleEmpresas,
  onToggleSocios,
  onReset,
}: GrafoControlesProps) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <span className="text-xs font-medium text-gray-500">Filtrar:</span>

      <label className="flex cursor-pointer items-center gap-1.5">
        <input
          type="checkbox"
          checked={showEmpresas}
          onChange={onToggleEmpresas}
          className="h-3.5 w-3.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
        <span className="text-xs text-gray-700">Empresas</span>
      </label>

      <label className="flex cursor-pointer items-center gap-1.5">
        <input
          type="checkbox"
          checked={showSocios}
          onChange={onToggleSocios}
          className="h-3.5 w-3.5 rounded border-gray-300 text-green-600 focus:ring-green-500"
        />
        <span className="text-xs text-gray-700">Sócios</span>
      </label>

      <Button variant="ghost" size="sm" onClick={onReset}>
        Resetar seleção
      </Button>
    </div>
  );
}
