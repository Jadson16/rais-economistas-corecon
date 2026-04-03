"""
LIMPEZA E PADRONIZAÇÃO — RAIS ECONOMISTAS (CBO 2512)
Estágio 2 da pipeline
Entrada : data/processed/rais_economistas.parquet
Saída   : data/processed/rais_economistas_clean.parquet
"""

from pathlib import Path
import pandas as pd

# ── CAMINHOS ─────────────────────────────────────────────────────────────────
DIR_PROC        = Path("data/processed")
ARQUIVO_ENTRADA = DIR_PROC / "rais_economistas.parquet"
ARQUIVO_SAIDA   = DIR_PROC / "rais_economistas_clean.parquet"

# ── LEITURA ───────────────────────────────────────────────────────────────────
print("Lendo dados...")
df = pd.read_parquet(ARQUIVO_ENTRADA)
print(f"  {len(df):,} vínculos carregados.")

# ── 1. REMOVER COLUNA VAZIA ───────────────────────────────────────────────────
# grau_instrucao_1985_2005 está 100% vazia na série 2006+
df = df.drop(columns=["grau_instrucao_1985_2005"])

# ── 2. NÍVEL GEOGRÁFICO ───────────────────────────────────────────────────────
NORDESTE = {"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"}

def nivel_geografico(uf):
    if uf == "MA":
        return "Maranhão"
    elif uf in NORDESTE:
        return "Nordeste"
    else:
        return "Brasil"

df["nivel_geografico"] = df["sigla_uf"].map(nivel_geografico)

# ── 3. SEXO ───────────────────────────────────────────────────────────────────
df["sexo"] = df["sexo"].map({"1": "Masculino", "2": "Feminino"}).fillna("Não informado")

# ── 4. CBO — DESCRIÇÃO ────────────────────────────────────────────────────────
CBO_DESC = {
    "251205": "Economista",
    "251210": "Economista agrícola",
    "251215": "Economista industrial",
    "251220": "Economista do trabalho",
    "251225": "Economista do setor público",
    "251230": "Economista ambiental",
    "251235": "Economista regional e urbano",
}
df["cbo_descricao"] = df["cbo_2002"].map(CBO_DESC).fillna("Outro")

# ── 5. GRAU DE INSTRUÇÃO ──────────────────────────────────────────────────────
INSTRUCAO = {
    "1":  "Analfabeto",
    "2":  "Até 5ª incompleto",
    "3":  "5ª completo fundamental",
    "4":  "6ª a 9ª fundamental",
    "5":  "Fundamental completo",
    "6":  "Médio incompleto",
    "7":  "Médio completo",
    "8":  "Superior incompleto",
    "9":  "Superior completo",
    "10": "Mestrado",
    "11": "Doutorado",
}
df["grau_instrucao"] = df["grau_instrucao_apos_2005"].map(INSTRUCAO).fillna("Não informado")
df = df.drop(columns=["grau_instrucao_apos_2005"])

# ── 6. RAÇA/COR ───────────────────────────────────────────────────────────────
RACA = {
    "1": "Indígena",
    "2": "Branca",
    "4": "Preta",
    "6": "Amarela",
    "8": "Parda",
    "9": "Não informado",
}
df["raca_cor"] = df["raca_cor"].map(RACA).fillna("Não informado")

# ── 7. SETOR PÚBLICO / PRIVADO ────────────────────────────────────────────────
# Natureza jurídica: códigos 1xxx = administração pública
df["setor"] = df["natureza_juridica"].apply(
    lambda x: "Público" if str(x).startswith("1") else "Privado"
    if pd.notna(x) else "Não informado"
)

# ── 8. TAMANHO DO ESTABELECIMENTO ─────────────────────────────────────────────
TAMANHO = {
    "0": "Zero",
    "1": "1 a 4",
    "2": "5 a 9",
    "3": "10 a 19",
    "4": "20 a 49",
    "5": "50 a 99",
    "6": "100 a 249",
    "7": "250 a 499",
    "8": "500 a 999",
    "9": "1000 ou mais",
}
df["tamanho_estabelecimento"] = df["tamanho_estabelecimento"].map(TAMANHO).fillna("Não informado")

# ── 9. REMUNERAÇÃO — TRATAMENTO DE OUTLIERS ───────────────────────────────────
# Remove remunerações zeradas ou negativas (erros de declaração)
n_antes = len(df)
df = df[df["valor_remuneracao_media"] > 0]
n_removidos = n_antes - len(df)
print(f"  Removidos {n_removidos:,} vínculos com remuneração <= 0.")

# ── 10. ORDENAÇÃO FINAL ───────────────────────────────────────────────────────
df = df.sort_values(["ano", "sigla_uf", "cbo_2002"]).reset_index(drop=True)

# ── 11. EXPORTAÇÃO ────────────────────────────────────────────────────────────
df.to_parquet(ARQUIVO_SAIDA, index=False)

# ── RELATÓRIO FINAL ───────────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("LIMPEZA CONCLUÍDA")
print("=" * 55)
print(f"Vínculos finais : {len(df):,}")
print(f"Colunas         : {df.shape[1]}")
print(f"Arquivo         : {ARQUIVO_SAIDA}")

print("\nDistribuição por nível geográfico:")
geo = df.groupby("nivel_geografico").size().reset_index(name="n")
geo["pct"] = (geo["n"] / len(df) * 100).round(1).astype(str) + "%"
print(geo.to_string(index=False))

print("\nDistribuição por sexo (Maranhão):")
ma = df[df["nivel_geografico"] == "Maranhão"]
print(ma["sexo"].value_counts().to_string())

print("\nRemuneração média por nível geográfico (R$):")
print(df.groupby("nivel_geografico")["valor_remuneracao_media"]
        .mean().round(2).to_string())

print("\nRemuneração média por nível geográfico e sexo (R$):")
print(df.groupby(["nivel_geografico", "sexo"])["valor_remuneracao_media"]
        .mean().round(2).to_string())
