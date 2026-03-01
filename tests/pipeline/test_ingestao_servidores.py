# tests/pipeline/test_ingestao_servidores.py
#
# Specification tests for Portal da Transparencia servidores ingestao.
from __future__ import annotations

from pathlib import Path

import polars as pl

from pipeline.sources.servidores.parse import extrair_digitos_visiveis, parse_servidores
from pipeline.sources.servidores.validate import validate_servidores

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_SERVIDORES = FIXTURES_DIR / "sample_servidores.csv"


def test_extrai_digitos_visiveis_de_cpf_mascarado() -> None:
    """'***.222.333-**' yields '222333' (6 visible digits)."""
    result = extrair_digitos_visiveis("***.222.333-**")
    assert result == "222333"


def test_extrai_digitos_visiveis_outro_exemplo() -> None:
    """'***.444.555-**' yields '444555'."""
    result = extrair_digitos_visiveis("***.444.555-**")
    assert result == "444555"


def test_extrai_digitos_visiveis_formato_invalido_retorna_none() -> None:
    """Strings that do not match the masked format return None."""
    assert extrair_digitos_visiveis("") is None
    assert extrair_digitos_visiveis("123.456.789-00") is None
    assert extrair_digitos_visiveis(None) is None


def test_parse_servidores_nome_normalizado() -> None:
    """Names are uppercased and stripped after parsing."""
    df = parse_servidores(SAMPLE_SERVIDORES)

    assert "nome" in df.columns
    for nome in df["nome"].drop_nulls():
        assert nome == nome.upper(), f"Expected uppercase, got: {nome}"
        assert nome == nome.strip(), f"Expected stripped, got: {nome}"


def test_parse_servidores_extrai_cargo_e_orgao() -> None:
    """cargo and orgao_lotacao are extracted from the CSV."""
    df = parse_servidores(SAMPLE_SERVIDORES)

    assert "cargo" in df.columns
    assert "orgao_lotacao" in df.columns
    cargos = df["cargo"].drop_nulls().to_list()
    assert len(cargos) > 0


def test_parse_servidores_digitos_visiveis_extraidos() -> None:
    """digitos_visiveis is populated from the masked CPF column."""
    df = parse_servidores(SAMPLE_SERVIDORES)

    assert "digitos_visiveis" in df.columns
    visiveis = df["digitos_visiveis"].drop_nulls().to_list()
    assert len(visiveis) > 0
    # All extracted values should be 6-digit strings
    for v in visiveis:
        assert len(v) == 6
        assert v.isdigit()


def test_validate_remove_duplicatas() -> None:
    """Dedup by (nome, digitos_visiveis) keeps first occurrence."""
    df = pl.DataFrame(
        {
            "nome": ["JOAO SILVA", "JOAO SILVA"],
            "cpf_mascarado": ["***.222.333-**", "***.222.333-**"],
            "digitos_visiveis": ["222333", "222333"],
            "cargo": ["TECNICO", "ANALISTA"],
            "orgao_lotacao": ["MEC", "MEC"],
            "is_servidor_publico": [True, True],
        }
    )
    result = validate_servidores(df)

    assert len(result) == 1
    assert result["cargo"][0] == "TECNICO"


def test_validate_rejeita_sem_nome() -> None:
    """Rows without nome are dropped."""
    df = pl.DataFrame(
        {
            "nome": [None, "MARIA OLIVEIRA"],
            "cpf_mascarado": ["***.111.222-**", "***.333.444-**"],
            "digitos_visiveis": ["111222", "333444"],
            "cargo": ["CARGO X", "ANALISTA"],
            "orgao_lotacao": ["ORG1", "ORG2"],
            "is_servidor_publico": [True, True],
        }
    )
    result = validate_servidores(df)

    assert len(result) == 1
    assert result["nome"][0] == "MARIA OLIVEIRA"


def test_download_extrai_cadastro_do_zip(tmp_path: Path) -> None:
    """ZIP with multiple CSVs extracts *Cadastro* (not first alphabetically)."""
    import zipfile

    from pipeline.sources.servidores.download import _extract_cadastro_csv

    zip_path = tmp_path / "servidores_202601.zip"
    csv_files = [
        "202601_Servidores_SIAPE_Afastamentos.csv",
        "202601_Servidores_SIAPE_Cadastro.csv",
        "202601_Servidores_SIAPE_Observacoes.csv",
        "202601_Servidores_SIAPE_Remuneracao.csv",
    ]
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name in csv_files:
            zf.writestr(name, "COL1;COL2\nval1;val2\n")

    result = _extract_cadastro_csv(zip_path, tmp_path)

    assert result.name == "202601_Servidores_SIAPE_Cadastro.csv"
    assert result.exists()
