"""
VERIFICAÇÃO PÓS-EXTRAÇÃO
Roda após download_bigquery.py para validar os dados antes do tratamento.
"""

from pathlib import Path
import pandas as pd

ARQUIVO = Path("data/processed/rais_economistas.parquet")

if not ARQUIVO.exists():
    print("Arquivo não encontrado. Rode download_bigquery.py primeiro.")
    exit()

df = pd.read_parquet(ARQUIVO)

DESCRICAO_CBO = {
    "251205": "Economista",
    "251210": "Economista agrícola",
    "251215": "Economista industrial",
    "251220": "Economista do trabalho",
    "251225": "Economista do setor público",
    "251230": "Economista ambiental",
    "251235": "Economista regional e urbano",
}

print("=" * 55)
print("RELATÓRIO DE VERIFICAÇÃO — RAIS ECONOMISTAS")
print("=" * 55)

print(f"\nDimensões: {len(df):,} vínculos × {df.shape[1]} colunas")

print("\nCobertura temporal:")
print(df["ano"].value_counts().sort_index().to_string())

print("\nCódigos CBO presentes:")
cbo_counts = df["cbo_2002"].value_counts().reset_index()
cbo_counts.columns = ["cbo_2002", "n"]
cbo_counts["descricao"] = cbo_counts["cbo_2002"].map(DESCRICAO_CBO).fillna("Outro")
cbo_counts["pct"] = (cbo_counts["n"] / len(df) * 100).round(1).astype(str) + "%"
print(cbo_counts.to_string(index=False))

print("\nDistribuição por sexo:")
print(df["sexo"].value_counts().to_string())

print("\nTop 10 UFs:")
print(df["sigla_uf"].value_counts().head(10).to_string())

print("\nMissings por variável:")
missings = df.isnull().sum()
missings = missings[missings > 0]
if missings.empty:
    print("  Nenhum missing encontrado.")
else:
    print(missings.to_string())

print("\nEstatísticas de remuneração (vl_remun_media_nom > 0):")
remun = df.loc[df["valor_remuneracao_media"] > 0, "valor_remuneracao_media"]
print(remun.describe().round(2).to_string())

print("\n" + "=" * 55)
print("Verificação concluída. Se os dados parecerem")
print("coerentes, prossiga para clean_rais.py")
print("=" * 55)
