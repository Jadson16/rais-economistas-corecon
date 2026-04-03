from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────
BILLING_PROJECT = "rais-economistas"
ANOS            = list(range(2006, 2023))
DIR_RAW         = Path("data/raw/rais_cbo")
DIR_PROC        = Path("data/processed")
DIR_RAW.mkdir(parents=True, exist_ok=True)
DIR_PROC.mkdir(parents=True, exist_ok=True)

# ── CLIENTE ───────────────────────────────────────────────────────────────────
client = bigquery.Client(project=BILLING_PROJECT)

# ── EXTRAÇÃO POR ANO ──────────────────────────────────────────────────────────
def extrair_ano(ano):
    saida = DIR_RAW / f"rais_economistas_{ano}.parquet"
    if saida.exists():
        print(f"Ano {ano}: já existe, pulando.")
        return

    print(f"Extraindo ano {ano}...", end=" ", flush=True)

    sql = (
        "SELECT "
        "ano, sigla_uf, id_municipio, cbo_2002, "
        "sexo, faixa_etaria, idade, "
        "grau_instrucao_apos_2005, grau_instrucao_1985_2005, "
        "raca_cor, nacionalidade, "
        "vinculo_ativo_3112, tipo_vinculo, tipo_admissao, "
        "natureza_juridica, tamanho_estabelecimento, tipo_estabelecimento, "
        "valor_remuneracao_media, valor_remuneracao_media_sm, "
        "valor_remuneracao_dezembro, "
        "faixa_remuneracao_media_sm, "
        "cnae_2, cnae_2_subclasse, "
        "quantidade_horas_contratadas, faixa_horas_contratadas, "
        "tempo_emprego, faixa_tempo_emprego "
        "FROM `basedosdados.br_me_rais.microdados_vinculos` "
        "WHERE ano = " + str(ano) + " "
        "AND vinculo_ativo_3112 = '1' "
        "AND cbo_2002 LIKE '2512%'"
    )

    try:
        df = client.query(sql).to_dataframe()
        df.to_parquet(saida, index=False)
        print(f"{len(df):,} vínculos salvos.")
    except Exception as e:
        print(f"\n  ERRO: {e}")

# ── EXECUÇÃO ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("EXTRAÇÃO RAIS — ECONOMISTAS (CBO 2512)")
    print("=" * 55)

    for ano in ANOS:
        extrair_ano(ano)

    print("\nConsolidando arquivos anuais...")
    arquivos = sorted(DIR_RAW.glob("rais_economistas_*.parquet"))

    if not arquivos:
        print("Nenhum arquivo encontrado para consolidar.")
    else:
        df_total = pd.concat(
            [pd.read_parquet(f) for f in arquivos],
            ignore_index=True
        )
        saida = DIR_PROC / "rais_economistas.parquet"
        df_total.to_parquet(saida, index=False)
        print(f"Concluído: {len(df_total):,} vínculos | {df_total['ano'].min()}–{df_total['ano'].max()}")
        print(f"Arquivo : {saida}")
