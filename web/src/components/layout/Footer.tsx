import { Link } from "react-router-dom";

export function Footer() {
  return (
    <footer className="border-t border-gray-200 bg-gray-50">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        <div className="flex flex-col items-center justify-between gap-4 sm:flex-row">
          <p className="text-xs text-gray-500">
            Dados p&uacute;blicos processados automaticamente. N&atilde;o constitui
            acusa&ccedil;&atilde;o. Consulte a{" "}
            <Link
              to="/metodologia"
              className="underline hover:text-gray-700"
            >
              metodologia
            </Link>
            .
          </p>
          <div className="flex gap-4">
            <Link
              to="/metodologia"
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              Metodologia
            </Link>
            <a
              href="https://github.com"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              GitHub
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
