import { createBrowserRouter } from "react-router-dom";
import { App } from "./App";
import { Home } from "./pages/Home/Home";
import { FichaFornecedor } from "./pages/FichaFornecedor/FichaFornecedor";
import { Ranking } from "./pages/Ranking/Ranking";
import { Busca } from "./pages/Busca/Busca";
import { Alertas } from "./pages/Alertas/Alertas";
import { GrafoSocietario } from "./pages/GrafoSocietario/GrafoSocietario";
import { DashboardOrgao } from "./pages/DashboardOrgao/DashboardOrgao";
import { Metodologia } from "./pages/Metodologia/Metodologia";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    children: [
      { index: true, element: <Home /> },
      { path: "fornecedores/:cnpj", element: <FichaFornecedor /> },
      { path: "ranking", element: <Ranking /> },
      { path: "busca", element: <Busca /> },
      { path: "alertas", element: <Alertas /> },
      { path: "fornecedores/:cnpj/grafo", element: <GrafoSocietario /> },
      { path: "orgaos/:codigo", element: <DashboardOrgao /> },
      { path: "metodologia", element: <Metodologia /> },
    ],
  },
]);
