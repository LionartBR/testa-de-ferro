# api/domain/sancao/value_objects.py
from enum import StrEnum


class TipoSancao(StrEnum):
    CEIS = "CEIS"    # Empresas Impedidas
    CNEP = "CNEP"    # Penalidades Lei Anticorrupcao
    CEPIM = "CEPIM"  # Entidades sem fins lucrativos
