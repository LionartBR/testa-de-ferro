# api/domain/servidor/value_objects.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Cargo:
    valor: str


@dataclass(frozen=True)
class OrgaoLotacao:
    valor: str
