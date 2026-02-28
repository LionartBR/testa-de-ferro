# api/application/dtos/export_dto.py
from typing import Literal

from pydantic import BaseModel


class ExportRequestDTO(BaseModel):
    formato: Literal["csv", "json", "pdf"]
