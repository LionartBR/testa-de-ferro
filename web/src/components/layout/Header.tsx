import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";

export function Header() {
  const [query, setQuery] = useState("");
  const navigate = useNavigate();

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = query.trim();
    if (trimmed) {
      navigate(`/busca?q=${encodeURIComponent(trimmed)}`);
      setQuery("");
    }
  }

  return (
    <header className="border-b border-gray-200 bg-white">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3 sm:px-6">
        <div className="flex items-center gap-8">
          <Link to="/" className="text-lg font-bold text-gray-900">
            Testa de Ferro
          </Link>
          <nav className="hidden items-center gap-6 md:flex">
            <Link
              to="/ranking"
              className="text-sm text-gray-600 hover:text-gray-900"
            >
              Ranking
            </Link>
            <Link
              to="/alertas"
              className="text-sm text-gray-600 hover:text-gray-900"
            >
              Alertas
            </Link>
            <Link
              to="/busca"
              className="text-sm text-gray-600 hover:text-gray-900"
            >
              Busca
            </Link>
            <Link
              to="/metodologia"
              className="text-sm text-gray-600 hover:text-gray-900"
            >
              Metodologia
            </Link>
          </nav>
        </div>

        <form onSubmit={handleSubmit} className="hidden sm:block">
          <input
            type="search"
            placeholder="Buscar CNPJ ou razão social..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            maxLength={200}
            className="w-64 rounded-md border border-gray-300 px-3 py-1.5 text-sm placeholder-gray-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </form>
      </div>
    </header>
  );
}
