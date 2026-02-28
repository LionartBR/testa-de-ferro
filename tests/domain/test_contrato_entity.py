# tests/domain/test_contrato_entity.py
from datetime import date
from decimal import Decimal

import pytest

from api.domain.contrato.entities import Contrato
from api.domain.contrato.value_objects import ValorContrato
from api.domain.fornecedor.value_objects import CNPJ


def test_contrato_com_dados_minimos():
    c = Contrato(
        fornecedor_cnpj=CNPJ("11222333000181"),
        orgao_codigo="00001",
        valor=ValorContrato(Decimal("500000.00")),
        data_assinatura=date(2025, 6, 1),
    )
    assert c.valor.valor == Decimal("500000.00")


def test_valor_contrato_negativo_invalido():
    with pytest.raises(ValueError, match="negativo"):
        ValorContrato(Decimal("-1"))


def test_valor_contrato_zero_valido():
    vc = ValorContrato(Decimal("0"))
    assert vc.valor == Decimal("0")
