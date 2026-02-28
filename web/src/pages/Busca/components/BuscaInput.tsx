interface BuscaInputProps {
  value: string;
  onChange: (value: string) => void;
}

const MAX_LENGTH = 200;

export function BuscaInput({ value, onChange }: BuscaInputProps) {
  return (
    <div className="relative">
      <div
        aria-hidden="true"
        className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3 text-gray-400"
      >
        {/* search icon as text per spec */}
        <span className="text-base leading-none">&#128269;</span>
      </div>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        maxLength={MAX_LENGTH}
        placeholder="CNPJ ou razão social..."
        className="block w-full rounded-lg border border-gray-300 bg-white py-3 pl-10 pr-4 text-sm text-gray-900 placeholder-gray-400 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
        autoFocus
      />
    </div>
  );
}
