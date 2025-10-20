# db_config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("PGHOST", "localhost")
    port: int = int(os.getenv("PGPORT", "5432"))
    user: str = os.getenv("PGUSER", "postgres")
    password: str = os.getenv("PGPASSWORD", "sva337rro")
    database: str = os.getenv("PGDATABASE", "saude_rr")

def as_dsn(cfg: DBConfig) -> str:
    return (
        f"host={cfg.host} port={cfg.port} dbname={cfg.database} "
        f"user={cfg.user} password={cfg.password}"
    )

def as_admin_dsn(cfg: DBConfig, admin_db: str = "postgres") -> str:
    # usado para criar o banco (conecta no 'postgres')
    return (
        f"host={cfg.host} port={cfg.port} dbname={admin_db} "
        f"user={cfg.user} password={cfg.password}"
    )
