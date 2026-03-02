# db_config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("DB_Host")
    port: int = int(os.getenv("DB_Port"))
    user: str = os.getenv("DB_user")
    password: str = os.getenv("DB_Password")
    database: str = os.getenv("DB_Database")

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
