# api/application/services/export_service.py
from __future__ import annotations

import csv
import io

from ..dtos.ficha_dto import FichaCompletaDTO


class ExportService:
    def exportar_json(self, ficha: FichaCompletaDTO) -> str:
        return ficha.model_dump_json(indent=2)

    def exportar_csv(self, ficha: FichaCompletaDTO) -> str:
        output = io.StringIO()

        # Dados cadastrais
        output.write("# DADOS CADASTRAIS\n")
        writer = csv.writer(output)
        writer.writerow(["Campo", "Valor"])
        writer.writerow(["CNPJ", ficha.cnpj])
        writer.writerow(["Razao Social", ficha.razao_social])
        writer.writerow(["Situacao", ficha.situacao])
        writer.writerow(["Data Abertura", ficha.data_abertura or ""])
        writer.writerow(["Capital Social", ficha.capital_social or ""])
        writer.writerow(["CNAE", ficha.cnae_principal or ""])
        output.write("\n")

        # Score
        if ficha.score:
            output.write("# SCORE DE RISCO\n")
            writer.writerow(["Score", ficha.score.valor])
            writer.writerow(["Faixa", ficha.score.faixa])
            for ind in ficha.score.indicadores:
                writer.writerow([f"Indicador: {ind.tipo}", f"Peso: {ind.peso}", ind.descricao])
            output.write("\n")

        # Alertas
        if ficha.alertas_criticos:
            output.write("# ALERTAS CRITICOS\n")
            writer.writerow(["Tipo", "Severidade", "Descricao"])
            for a in ficha.alertas_criticos:
                writer.writerow([a.tipo, a.severidade, a.descricao])
            output.write("\n")

        # Contratos
        if ficha.contratos:
            output.write("# CONTRATOS\n")
            writer.writerow(["Orgao", "Valor", "Data Assinatura", "Objeto"])
            for c in ficha.contratos:
                writer.writerow([c.orgao_codigo, c.valor, c.data_assinatura or "", c.objeto or ""])
            output.write("\n")

        # Socios
        if ficha.socios:
            output.write("# SOCIOS\n")
            writer.writerow(["Nome", "Qualificacao", "Servidor Publico", "Orgao"])
            for s in ficha.socios:
                writer.writerow([s.nome, s.qualificacao or "", s.is_servidor_publico, s.orgao_lotacao or ""])

        return output.getvalue()
