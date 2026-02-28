# api/infrastructure/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    duckdb_path: str
    cpf_hmac_salt: str
    rate_limit_per_minute: int
    debug: bool


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        duckdb_path=os.environ.get("DUCKDB_PATH", ":memory:"),
        cpf_hmac_salt=os.environ.get("CPF_HMAC_SALT", ""),
        rate_limit_per_minute=int(os.environ.get("API_RATE_LIMIT_PER_MINUTE", "60")),
        debug=os.environ.get("API_DEBUG", "false").lower() == "true",
    )
