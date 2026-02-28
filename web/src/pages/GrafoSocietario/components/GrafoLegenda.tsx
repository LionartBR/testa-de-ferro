// Placeholder legend — will be enhanced when a force-directed lib is added.

const LEGEND_ITEMS = [
  { color: "bg-blue-500", label: "Empresa" },
  { color: "bg-green-500", label: "Sócio / Pessoa Física" },
] as const;

export function GrafoLegenda() {
  return (
    <div className="flex items-center gap-4">
      {LEGEND_ITEMS.map((item) => (
        <div key={item.label} className="flex items-center gap-1.5">
          <span className={`inline-block h-3 w-3 rounded-full ${item.color}`} />
          <span className="text-xs text-gray-600">{item.label}</span>
        </div>
      ))}
    </div>
  );
}
