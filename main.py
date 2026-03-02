# main.py
import subprocess
import sys
import argparse

def run(script_name, args=None):
    """
    Executa um script python usando o mesmo interpretador atual.
    """
    cmd = [sys.executable, script_name]
    if args and getattr(args, 'force', False):
        cmd.append("--force")
    
    print(f"\n$ {' '.join(cmd)}")
    code = subprocess.call(cmd)
    if code != 0:
        print(f"❌ Erro ao executar {script_name} (código {code})")
        sys.exit(code)

def main():
    parser = argparse.ArgumentParser(description="Painel de Saúde - Pipeline de Dados")
    parser.add_argument("--force", action="store_true", help="Reprocessa dados já existentes no banco")
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--db", action="store_true", help="Cria/atualiza apenas o banco e tabelas")
    group.add_argument("--cnes", action="store_true", help="Executa apenas a carga do CNES (leitos, equipamentos, unidades)")
    group.add_argument("--siops", action="store_true", help="Executa apenas a carga do SIOPS")
    group.add_argument("--all", action="store_true", help="Executa todo o pipeline (padrão se nenhum outro for escolhido)")

    args = parser.parse_args()

    # Se nenhum argumento de ação for passado, assume --all
    run_all = args.all or (not args.db and not args.cnes and not args.siops)

    if args.db or run_all:
        print("--- [1/3] Banco de Dados ---")
        run("create_db_and_tables.py")

    if args.cnes or run_all:
        print("--- [2/3] Carga CNES ---")
        run("cnes_tipo_leito_to_pg.py", args)
        run("cnes_equipamentos_to_pg.py", args)
        run("cnes_tipo_unidade_to_pg.py", args)

    if args.siops or run_all:
        print("--- [3/3] Carga SIOPS ---")
        run("siops_to_pg.py", args)

    print("\n🎉 Pipeline finalizado com sucesso.")

if __name__ == "__main__":
    main()
