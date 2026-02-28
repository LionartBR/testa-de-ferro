# api/interfaces/api/routes/export_routes.py
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

from api.application.services.export_service import ExportService
from api.application.services.ficha_service import FichaService
from api.domain.fornecedor.value_objects import CNPJ
from api.interfaces.api.dependencies import get_export_service, get_ficha_service

router = APIRouter()


@router.get("/fornecedores/{cnpj_raw}/export")
def export_ficha(
    cnpj_raw: str,
    formato: Literal["csv", "json", "pdf"] = Query(...),
    ficha_service: FichaService = Depends(get_ficha_service),  # noqa: B008
    export_service: ExportService = Depends(get_export_service),  # noqa: B008
) -> Response:
    try:
        cnpj = CNPJ(cnpj_raw)
    except ValueError as err:
        raise HTTPException(status_code=422, detail="CNPJ invalido") from err

    ficha = ficha_service.obter_ficha(cnpj)
    if ficha is None:
        raise HTTPException(status_code=404, detail="Fornecedor nao encontrado")

    if formato == "json":
        return Response(
            content=export_service.exportar_json(ficha),
            media_type="application/json",
        )
    if formato == "csv":
        return Response(
            content=export_service.exportar_csv(ficha),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={cnpj.valor}.csv"},
        )
    # pdf
    try:
        from api.infrastructure.pdf_generator import gerar_pdf_ficha

        pdf_bytes = gerar_pdf_ficha(ficha)
    except RuntimeError as err:
        raise HTTPException(status_code=501, detail=str(err)) from err
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={cnpj.valor}.pdf"},
    )
