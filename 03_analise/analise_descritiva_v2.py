"""
ANÁLISE DESCRITIVA v2 — RAIS ECONOMISTAS (CBO 2512)
Blocos adicionais:
  9  — Concentração geográfica intraestadual (municípios do MA)
  10 — Distribuição e evolução por faixa etária
  11 — Cruzamento setor público/privado × remuneração
Entrada : data/processed/rais_economistas_clean.parquet
Saída   : outputs/tabelas/ e outputs/graficos/
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import warnings
warnings.filterwarnings("ignore")

# ── CAMINHOS ──────────────────────────────────────────────────────────────────
ARQUIVO      = Path("data/processed/rais_economistas_clean.parquet")
DIR_TABELAS  = Path("outputs/tabelas")
DIR_GRAFICOS = Path("outputs/graficos")
DIR_TABELAS.mkdir(parents=True, exist_ok=True)
DIR_GRAFICOS.mkdir(parents=True, exist_ok=True)

# ── ESTILO ────────────────────────────────────────────────────────────────────
CORES = {
    "Maranhão": "#1a6b9a",
    "Nordeste":  "#e07b39",
    "Brasil":    "#4caf7d",
    "Público":   "#1a6b9a",
    "Privado":   "#e07b39",
}
ORDEM_GEO = ["Maranhão", "Nordeste", "Brasil"]

plt.rcParams.update({
    "figure.dpi":        150,
    "font.family":       "sans-serif",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.titlesize":    12,
    "axes.labelsize":    10,
    "xtick.labelsize":   9,
    "ytick.labelsize":   9,
})

# ── LEITURA ───────────────────────────────────────────────────────────────────
print("Lendo dados...")
df = pd.read_parquet(ARQUIVO)
df_ma = df[df["nivel_geografico"] == "Maranhão"].copy()
ano_max = df["ano"].max()
print(f"  {len(df):,} vínculos | MA: {len(df_ma):,} | ano mais recente: {ano_max}\n")

# ── MAPA DE MUNICÍPIOS DO MA ──────────────────────────────────────────────────
# Nomes dos principais municípios do MA pelo código IBGE (7 dígitos)
MUNICIPIOS_MA = {
    "2111300": "São Luís",
    "2105302": "Imperatriz",
    "2111706": "São José de Ribamar",
    "2109700": "Paço do Lumiar",
    "2101400": "Açailândia",
    "2112209": "Timon",
    "2104701": "Caxias",
    "2103000": "Bacabal",
    "2107803": "Codó",
    "2114007": "Zé Doca",
    "2105401": "Itapecuru Mirim",
    "2106805": "Balsas",
    "2108405": "Santa Inês",
    "2101509": "Açailândia",
    "2109403": "Pinheiro",
}

# =============================================================================
# BLOCO 9 — CONCENTRAÇÃO GEOGRÁFICA INTRAESTADUAL (MA)
# =============================================================================
print("Bloco 9: Concentração geográfica intraestadual...")

# Série histórica por município — top 10 no ano mais recente
municipios_ano = (
    df_ma.groupby(["ano", "id_municipio"])
    .size()
    .reset_index(name="n")
)

# Top 10 municípios no ano mais recente
top10_mun = (
    municipios_ano[municipios_ano["ano"] == ano_max]
    .nlargest(10, "n")
    .copy()
)
top10_mun["nome"] = top10_mun["id_municipio"].map(MUNICIPIOS_MA).fillna(
    top10_mun["id_municipio"]
)
total_ma_ano = top10_mun["n"].sum()
top10_mun["pct"] = (top10_mun["n"] / len(df_ma[df_ma["ano"] == ano_max]) * 100).round(1)
top10_mun.to_csv(DIR_TABELAS / f"09_top10_municipios_MA_{ano_max}.csv", index=False)

# Gráfico — concentração no ano mais recente
fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.barh(
    top10_mun["nome"],
    top10_mun["pct"],
    color=CORES["Maranhão"],
    alpha=0.85
)
for bar, val in zip(bars, top10_mun["pct"]):
    ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}%", va="center", fontsize=8)
ax.set_xlabel("Participação no estoque estadual (%)")
ax.set_title(f"Concentração municipal dos economistas — Maranhão ({ano_max})")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"09_concentracao_municipal_MA_{ano_max}.png")
plt.close()

# Evolução da concentração em São Luís ao longo do tempo
sl_codigo = "2111300"
evolucao_sl = municipios_ano[municipios_ano["id_municipio"] == sl_codigo].copy()
total_por_ano = municipios_ano.groupby("ano")["n"].sum().reset_index(name="total")
evolucao_sl = evolucao_sl.merge(total_por_ano, on="ano")
evolucao_sl["pct_sl"] = (evolucao_sl["n"] / evolucao_sl["total"] * 100).round(1)
evolucao_sl.to_csv(DIR_TABELAS / "09b_evolucao_sao_luis.csv", index=False)

fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()
ax1.bar(evolucao_sl["ano"], evolucao_sl["n"],
        color=CORES["Maranhão"], alpha=0.6, label="Vínculos em São Luís")
ax2.plot(evolucao_sl["ano"], evolucao_sl["pct_sl"],
         color="black", linewidth=2, marker="o", markersize=5,
         label="% do estado")
ax1.set_xlabel("Ano")
ax1.set_ylabel("Vínculos — São Luís", color=CORES["Maranhão"])
ax2.set_ylabel("Participação no estado (%)", color="black")
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax1.set_title("São Luís: estoque e concentração no Maranhão (2006–2022)")
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "09b_evolucao_sao_luis.png")
plt.close()

# =============================================================================
# BLOCO 10 — FAIXA ETÁRIA
# =============================================================================
print("Bloco 10: Faixa etária...")

ORDEM_FAIXA = [
    "10 a 14", "15 a 17", "18 a 24", "25 a 29", "30 a 39",
    "40 a 49", "50 a 64", "65 ou mais"
]

# Decodifica faixa etária (código RAIS → descrição)
FAIXA_ETARIA = {
    "1": "10 a 14",
    "2": "15 a 17",
    "3": "18 a 24",
    "4": "25 a 29",
    "5": "30 a 39",
    "6": "40 a 49",
    "7": "50 a 64",
    "8": "65 ou mais",
}
df["faixa_etaria_desc"] = df["faixa_etaria"].map(FAIXA_ETARIA).fillna("Não informado")
df_ma["faixa_etaria_desc"] = df_ma["faixa_etaria"].map(FAIXA_ETARIA).fillna("Não informado")

# Distribuição por faixa etária no ano mais recente — três níveis
faixa_dist = (
    df[df["ano"] == ano_max]
    .groupby(["nivel_geografico", "faixa_etaria_desc"])
    .size()
    .reset_index(name="n")
)
total_faixa = faixa_dist.groupby("nivel_geografico")["n"].transform("sum")
faixa_dist["pct"] = (faixa_dist["n"] / total_faixa * 100).round(1)
faixa_dist.to_csv(DIR_TABELAS / f"10_faixa_etaria_{ano_max}.csv", index=False)

fig, axes = plt.subplots(1, 3, figsize=(14, 5), sharey=True)
for ax, nivel in zip(axes, ORDEM_GEO):
    dados = (
        faixa_dist[faixa_dist["nivel_geografico"] == nivel]
        .set_index("faixa_etaria_desc")
        .reindex([f for f in ORDEM_FAIXA if f in faixa_dist["faixa_etaria_desc"].values])
        .reset_index()
    )
    ax.barh(dados["faixa_etaria_desc"], dados["pct"],
            color=CORES[nivel], alpha=0.85)
    ax.set_title(nivel, color=CORES[nivel], fontweight="bold")
    ax.set_xlabel("Participação (%)")
    for i, val in enumerate(dados["pct"]):
        ax.text(val + 0.2, i, f"{val:.1f}%", va="center", fontsize=7)

fig.suptitle(f"Distribuição por faixa etária — {ano_max}", fontsize=12)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"10_faixa_etaria_{ano_max}.png")
plt.close()

# Evolução da faixa 30-39 e 25-29 no MA (faixas mais representativas)
faixa_evolucao = (
    df_ma.groupby(["ano", "faixa_etaria_desc"])
    .size()
    .reset_index(name="n")
)
total_faixa_ano = faixa_evolucao.groupby("ano")["n"].transform("sum")
faixa_evolucao["pct"] = (faixa_evolucao["n"] / total_faixa_ano * 100).round(1)
faixa_evolucao.to_csv(DIR_TABELAS / "10b_faixa_etaria_MA_evolucao.csv", index=False)

faixas_destaque = ["25 a 29", "30 a 39", "40 a 49", "50 a 64"]
cores_faixa = ["#1a6b9a", "#e07b39", "#4caf7d", "#9b59b6"]

fig, ax = plt.subplots(figsize=(10, 5))
for faixa, cor in zip(faixas_destaque, cores_faixa):
    dados = faixa_evolucao[faixa_evolucao["faixa_etaria_desc"] == faixa]
    if not dados.empty:
        ax.plot(dados["ano"], dados["pct"], color=cor, linewidth=2,
                marker="o", markersize=5, label=faixa)
ax.set_xlabel("Ano")
ax.set_ylabel("Participação na faixa (%)")
ax.set_title("Evolução da estrutura etária dos economistas — Maranhão (2006–2022)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "10b_faixa_etaria_MA_evolucao.png")
plt.close()

# =============================================================================
# BLOCO 11 — SETOR PÚBLICO/PRIVADO × REMUNERAÇÃO
# =============================================================================
print("Bloco 11: Setor público/privado × remuneração...")

# 11a — Remuneração média por setor e nível geográfico (último ano)
remun_setor = (
    df[df["ano"] == ano_max]
    .groupby(["nivel_geografico", "setor"])["valor_remuneracao_media"]
    .agg(["mean", "median", "count"])
    .round(2)
    .reset_index()
)
remun_setor.columns = ["nivel_geografico", "setor", "media", "mediana", "n"]
remun_setor.to_csv(DIR_TABELAS / f"11_remun_setor_{ano_max}.csv", index=False)

# Gráfico — média e mediana por setor nos três níveis
fig, axes = plt.subplots(1, 3, figsize=(13, 5), sharey=False)
for ax, nivel in zip(axes, ORDEM_GEO):
    dados = remun_setor[
        (remun_setor["nivel_geografico"] == nivel) &
        (remun_setor["setor"].isin(["Público", "Privado"]))
    ]
    x = range(len(dados))
    width = 0.35
    bars1 = ax.bar([i - width/2 for i in x], dados["media"],
                   width, label="Média", color=CORES[nivel], alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], dados["mediana"],
                   width, label="Mediana", color=CORES[nivel], alpha=0.45)
    ax.set_xticks(list(x))
    ax.set_xticklabels(dados["setor"])
    ax.set_title(nivel, color=CORES[nivel], fontweight="bold")
    ax.set_ylabel("R$ (nominal)")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _: f"R${v:,.0f}")
    )
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                f"R${bar.get_height():,.0f}", ha="center", fontsize=7, rotation=45)
    if ax == axes[0]:
        ax.legend(fontsize=8)

fig.suptitle(f"Remuneração por setor (público/privado) — {ano_max}", fontsize=12)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"11_remun_setor_{ano_max}.png")
plt.close()

# 11b — Evolução da remuneração por setor no MA
remun_setor_ma = (
    df_ma[df_ma["setor"].isin(["Público", "Privado"])]
    .groupby(["ano", "setor"])["valor_remuneracao_media"]
    .mean()
    .reset_index()
)
remun_setor_ma.to_csv(DIR_TABELAS / "11b_remun_setor_MA_evolucao.csv", index=False)

fig, ax = plt.subplots(figsize=(10, 5))
for setor, cor in CORES.items():
    if setor not in ["Público", "Privado"]:
        continue
    dados = remun_setor_ma[remun_setor_ma["setor"] == setor]
    ax.plot(dados["ano"], dados["valor_remuneracao_media"],
            color=cor, linewidth=2.5, marker="o", markersize=5, label=setor)
ax.set_xlabel("Ano")
ax.set_ylabel("Remuneração média (R$ nominal)")
ax.set_title("Remuneração média por setor — Maranhão (2006–2022)")
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda v, _: f"R${v:,.0f}")
)
ax.legend(fontsize=9)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "11b_remun_setor_MA_evolucao.png")
plt.close()

# 11c — Composição por setor e sexo no MA (último ano)
setor_sexo = (
    df_ma[df_ma["ano"] == ano_max]
    .groupby(["setor", "sexo"])
    .size()
    .reset_index(name="n")
)
total_setor_sexo = setor_sexo.groupby("setor")["n"].transform("sum")
setor_sexo["pct"] = (setor_sexo["n"] / total_setor_sexo * 100).round(1)
setor_sexo.to_csv(DIR_TABELAS / f"11c_setor_sexo_MA_{ano_max}.csv", index=False)

fig, ax = plt.subplots(figsize=(7, 4))
setores = ["Público", "Privado"]
x = range(len(setores))
width = 0.35
for i, sexo in enumerate(["Feminino", "Masculino"]):
    cor = "#e07b39" if sexo == "Feminino" else "#1a6b9a"
    valores = [
        setor_sexo.loc[
            (setor_sexo["setor"] == s) & (setor_sexo["sexo"] == sexo), "pct"
        ].values[0] if len(setor_sexo.loc[
            (setor_sexo["setor"] == s) & (setor_sexo["sexo"] == sexo)
        ]) > 0 else 0
        for s in setores
    ]
    offset = -width/2 if i == 0 else width/2
    bars = ax.bar([j + offset for j in x], valores, width,
                  label=sexo, color=cor, alpha=0.85)
    for bar, val in zip(bars, valores):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + 0.5,
                f"{val:.1f}%", ha="center", fontsize=8)

ax.set_xticks(list(x))
ax.set_xticklabels(setores)
ax.set_ylabel("Participação no setor (%)")
ax.set_title(f"Composição por sexo e setor — Maranhão ({ano_max})")
ax.legend(fontsize=9)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"11c_setor_sexo_MA_{ano_max}.png")
plt.close()

# 11d — Remuneração média por setor e sexo nos três níveis (último ano)
remun_setor_sexo = (
    df[(df["ano"] == ano_max) & (df["setor"].isin(["Público", "Privado"]))]
    .groupby(["nivel_geografico", "setor", "sexo"])["valor_remuneracao_media"]
    .mean()
    .round(2)
    .reset_index()
)
remun_setor_sexo.to_csv(
    DIR_TABELAS / f"11d_remun_setor_sexo_{ano_max}.csv", index=False
)

# =============================================================================
# RELATÓRIO FINAL
# =============================================================================
print("\n" + "=" * 55)
print("ANÁLISE DESCRITIVA v2 CONCLUÍDA")
print("=" * 55)
print(f"Tabelas  : {DIR_TABELAS}")
print(f"Gráficos : {DIR_GRAFICOS}")
print("\nArquivos gerados nesta rodada:")
novos = ["09", "10", "11"]
for f in sorted(DIR_TABELAS.glob("*.csv")):
    if any(f.name.startswith(p) for p in novos):
        print(f"  {f.name}")
for f in sorted(DIR_GRAFICOS.glob("*.png")):
    if any(f.name.startswith(p) for p in novos):
        print(f"  {f.name}")
