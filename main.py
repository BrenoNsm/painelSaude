# main.py
import subprocess, sys

def run(cmd):
    print(f"\n$ {cmd}")
    code = subprocess.call(cmd, shell=True)
    if code != 0:
        sys.exit(code)

if __name__ == "__main__":
    # 1) cria banco e tabelas (idempotente)
    run("python create_db_and_tables.py")

    # 2) popula CNES (3 coleções)
    run("python cnes_tipo_leito_to_pg.py")
    run("python cnes_equipamentos_to_pg.py")
    run("python cnes_tipo_unidade_to_pg.py")

    # 3) popula SIOPS
    run("python siops_to_pg.py")

    print("\n🎉 Finalizado.")
