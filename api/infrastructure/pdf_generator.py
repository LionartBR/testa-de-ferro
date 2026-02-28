# api/infrastructure/pdf_generator.py
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from api.application.dtos.ficha_dto import FichaCompletaDTO


def gerar_pdf_ficha(ficha: FichaCompletaDTO) -> bytes:
    """Generate a PDF report for a fornecedor ficha.

    Raises RuntimeError if weasyprint is not installed.
    """
    try:
        from weasyprint import HTML  # type: ignore[import-untyped,import-not-found]
    except ImportError as err:
        msg = "PDF export requires weasyprint. Install with: pip install testa-de-ferro[pdf]"
        raise RuntimeError(msg) from err

    html = _build_html(ficha)
    return HTML(string=html).write_pdf()  # type: ignore[no-any-return]


def _build_html(ficha: FichaCompletaDTO) -> str:
    sections: list[str] = []

    # Header
    sections.append(f"""
    <h1>Ficha do Fornecedor</h1>
    <p class="disclaimer">{ficha.disclaimer}</p>
    """)

    # Dados cadastrais
    endereco_str = ""
    if ficha.endereco:
        parts = [
            ficha.endereco.get("logradouro", ""),
            ficha.endereco.get("municipio", ""),
            ficha.endereco.get("uf", ""),
            ficha.endereco.get("cep", ""),
        ]
        endereco_str = ", ".join(p for p in parts if p)

    sections.append(f"""
    <h2>Dados Cadastrais</h2>
    <table>
        <tr><td class="label">CNPJ</td><td>{ficha.cnpj}</td></tr>
        <tr><td class="label">Razao Social</td><td>{ficha.razao_social}</td></tr>
        <tr><td class="label">Situacao</td><td>{ficha.situacao}</td></tr>
        <tr><td class="label">Data Abertura</td><td>{ficha.data_abertura or "-"}</td></tr>
        <tr><td class="label">Capital Social</td><td>R$ {ficha.capital_social or "-"}</td></tr>
        <tr><td class="label">CNAE</td><td>{ficha.cnae_principal or "-"} {ficha.cnae_descricao or ""}</td></tr>
        <tr><td class="label">Endereco</td><td>{endereco_str or "-"}</td></tr>
        <tr><td class="label">Total Contratos</td><td>{ficha.total_contratos}</td></tr>
        <tr><td class="label">Valor Total</td><td>R$ {ficha.valor_total_contratos}</td></tr>
    </table>
    """)

    # Score
    if ficha.score:
        ind_rows = "".join(
            f"<tr><td>{i.tipo}</td><td>{i.peso}</td><td>{i.descricao}</td></tr>" for i in ficha.score.indicadores
        )
        sections.append(f"""
        <h2>Score de Risco</h2>
        <p><strong>Score:</strong> {ficha.score.valor} &mdash; <strong>Faixa:</strong> {ficha.score.faixa}</p>
        <table>
            <tr><th>Indicador</th><th>Peso</th><th>Descricao</th></tr>
            {ind_rows}
        </table>
        """)

    # Alertas
    if ficha.alertas_criticos:
        alerta_rows = "".join(
            f"<tr><td>{a.tipo}</td><td>{a.severidade}</td><td>{a.descricao}</td></tr>" for a in ficha.alertas_criticos
        )
        sections.append(f"""
        <h2>Alertas Criticos</h2>
        <table>
            <tr><th>Tipo</th><th>Severidade</th><th>Descricao</th></tr>
            {alerta_rows}
        </table>
        """)

    # Contratos
    if ficha.contratos:
        contrato_rows = "".join(
            f"<tr><td>{c.orgao_codigo}</td><td>R$ {c.valor}</td><td>{c.data_assinatura or '-'}</td><td>{c.objeto or '-'}</td></tr>"
            for c in ficha.contratos
        )
        sections.append(f"""
        <h2>Contratos</h2>
        <table>
            <tr><th>Orgao</th><th>Valor</th><th>Data</th><th>Objeto</th></tr>
            {contrato_rows}
        </table>
        """)

    # Socios
    if ficha.socios:
        socio_rows = "".join(
            f"<tr><td>{s.nome}</td><td>{s.qualificacao or '-'}</td>"
            f"<td>{'Sim' if s.is_servidor_publico else 'Nao'}</td>"
            f"<td>{s.orgao_lotacao or '-'}</td></tr>"
            for s in ficha.socios
        )
        sections.append(f"""
        <h2>Socios</h2>
        <table>
            <tr><th>Nome</th><th>Qualificacao</th><th>Servidor</th><th>Orgao</th></tr>
            {socio_rows}
        </table>
        """)

    # Sancoes
    if ficha.sancoes:
        sancao_rows = "".join(
            f"<tr><td>{s.tipo}</td><td>{s.orgao_sancionador or '-'}</td>"
            f"<td>{s.data_inicio}</td><td>{s.data_fim or 'Vigente'}</td></tr>"
            for s in ficha.sancoes
        )
        sections.append(f"""
        <h2>Sancoes</h2>
        <table>
            <tr><th>Tipo</th><th>Orgao</th><th>Inicio</th><th>Fim</th></tr>
            {sancao_rows}
        </table>
        """)

    # Doacoes
    if ficha.doacoes:
        doacao_rows = "".join(
            f"<tr><td>{d.candidato_nome}</td><td>{d.candidato_partido or '-'}</td>"
            f"<td>R$ {d.valor}</td><td>{d.ano_eleicao}</td></tr>"
            for d in ficha.doacoes
        )
        sections.append(f"""
        <h2>Doacoes Eleitorais</h2>
        <table>
            <tr><th>Candidato</th><th>Partido</th><th>Valor</th><th>Ano</th></tr>
            {doacao_rows}
        </table>
        """)

    body = "\n".join(sections)

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Ficha - {ficha.cnpj}</title>
<style>
    body {{ font-family: Arial, sans-serif; margin: 40px; font-size: 11px; color: #333; }}
    h1 {{ font-size: 18px; border-bottom: 2px solid #333; padding-bottom: 8px; }}
    h2 {{ font-size: 14px; margin-top: 24px; color: #555; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background-color: #f5f5f5; font-weight: bold; }}
    .label {{ font-weight: bold; width: 180px; background-color: #f9f9f9; }}
    .disclaimer {{ font-size: 10px; color: #888; font-style: italic; margin-bottom: 16px; }}
</style>
</head>
<body>
{body}
</body>
</html>"""
