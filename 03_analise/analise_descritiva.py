"""
ANÁLISE DESCRITIVA — RAIS ECONOMISTAS (CBO 2512)
Estágio 3 da pipeline
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

# ── CONFIGURAÇÃO DE ESTILO ────────────────────────────────────────────────────
CORES = {
    "Maranhão": "#1a6b9a",
    "Nordeste": "#e07b39",
    "Brasil":   "#4caf7d",
}
ORDEM_GEO = ["Maranhão", "Nordeste", "Brasil"]

plt.rcParams.update({
    "figure.dpi":       150,
    "font.family":      "sans-serif",
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "axes.titlesize":   12,
    "axes.labelsize":   10,
    "xtick.labelsize":  9,
    "ytick.labelsize":  9,
})

# ── LEITURA ───────────────────────────────────────────────────────────────────
print("Lendo dados...")
df = pd.read_parquet(ARQUIVO)
print(f"  {len(df):,} vínculos carregados.\n")

# =============================================================================
# BLOCO 1 — EVOLUÇÃO DO ESTOQUE DE ECONOMISTAS
# =============================================================================
print("Bloco 1: Evolução do estoque...")

estoque = (
    df.groupby(["ano", "nivel_geografico"])
    .size()
    .reset_index(name="n")
)

# Tabela
tab_estoque = estoque.pivot(index="ano", columns="nivel_geografico", values="n")[ORDEM_GEO]
tab_estoque.to_csv(DIR_TABELAS / "01_estoque_por_ano.csv")

# Gráfico — MA em destaque, Nordeste e Brasil no eixo secundário
fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()

for nivel, cor in CORES.items():
    dados = estoque[estoque["nivel_geografico"] == nivel]
    if nivel == "Maranhão":
        ax1.plot(dados["ano"], dados["n"], color=cor, linewidth=2.5,
                 marker="o", markersize=5, label=nivel, zorder=3)
    else:
        ax2.plot(dados["ano"], dados["n"], color=cor, linewidth=1.5,
                 linestyle="--", marker="s", markersize=4, label=nivel)

ax1.set_xlabel("Ano")
ax1.set_ylabel("Vínculos — Maranhão", color=CORES["Maranhão"])
ax2.set_ylabel("Vínculos — Nordeste / Brasil", color="gray")
ax1.set_title("Evolução do estoque de economistas formais (2006–2022)")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)

plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "01_estoque_evolucao.png")
plt.close()

# =============================================================================
# BLOCO 2 — COMPOSIÇÃO POR GÊNERO
# =============================================================================
print("Bloco 2: Composição por gênero...")

genero = (
    df.groupby(["ano", "nivel_geografico", "sexo"])
    .size()
    .reset_index(name="n")
)
total = genero.groupby(["ano", "nivel_geografico"])["n"].transform("sum")
genero["pct"] = genero["n"] / total * 100

# Razão de feminização (% feminino)
feminizacao = (
    genero[genero["sexo"] == "Feminino"]
    .pivot(index="ano", columns="nivel_geografico", values="pct")[ORDEM_GEO]
)
feminizacao.to_csv(DIR_TABELAS / "02_razao_feminizacao.csv")

fig, ax = plt.subplots(figsize=(10, 5))
for nivel in ORDEM_GEO:
    ax.plot(feminizacao.index, feminizacao[nivel],
            color=CORES[nivel], linewidth=2 if nivel == "Maranhão" else 1.5,
            linestyle="-" if nivel == "Maranhão" else "--",
            marker="o" if nivel == "Maranhão" else "s",
            markersize=5 if nivel == "Maranhão" else 4,
            label=nivel)

ax.axhline(50, color="gray", linestyle=":", linewidth=1, label="Paridade (50%)")
ax.set_xlabel("Ano")
ax.set_ylabel("Participação feminina (%)")
ax.set_title("Razão de feminização dos economistas formais (2006–2022)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "02_feminizacao.png")
plt.close()

# =============================================================================
# BLOCO 3 — REMUNERAÇÃO MÉDIA POR ANO E NÍVEL GEOGRÁFICO
# =============================================================================
print("Bloco 3: Remuneração...")

remun = (
    df.groupby(["ano", "nivel_geografico"])["valor_remuneracao_media"]
    .mean()
    .reset_index()
)
tab_remun = remun.pivot(index="ano", columns="nivel_geografico",
                        values="valor_remuneracao_media")[ORDEM_GEO].round(2)
tab_remun.to_csv(DIR_TABELAS / "03_remuneracao_media.csv")

fig, ax = plt.subplots(figsize=(10, 5))
for nivel in ORDEM_GEO:
    dados = remun[remun["nivel_geografico"] == nivel]
    ax.plot(dados["ano"], dados["valor_remuneracao_media"],
            color=CORES[nivel], linewidth=2 if nivel == "Maranhão" else 1.5,
            linestyle="-" if nivel == "Maranhão" else "--",
            marker="o" if nivel == "Maranhão" else "s",
            markersize=5 if nivel == "Maranhão" else 4,
            label=nivel)

ax.set_xlabel("Ano")
ax.set_ylabel("Remuneração média (R$ nominal)")
ax.set_title("Remuneração média dos economistas formais (2006–2022)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R$ {x:,.0f}"))
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "03_remuneracao_evolucao.png")
plt.close()

# =============================================================================
# BLOCO 4 — GAP DE GÊNERO NA REMUNERAÇÃO
# =============================================================================
print("Bloco 4: Gap de gênero na remuneração...")

gap = (
    df.groupby(["ano", "nivel_geografico", "sexo"])["valor_remuneracao_media"]
    .mean()
    .reset_index()
)
gap_pivot = gap.pivot_table(index=["ano", "nivel_geografico"],
                            columns="sexo",
                            values="valor_remuneracao_media").reset_index()
gap_pivot["gap_pct"] = (gap_pivot["Feminino"] / gap_pivot["Masculino"] * 100).round(1)
gap_pivot.to_csv(DIR_TABELAS / "04_gap_genero_remuneracao.csv", index=False)

fig, ax = plt.subplots(figsize=(10, 5))
for nivel in ORDEM_GEO:
    dados = gap_pivot[gap_pivot["nivel_geografico"] == nivel]
    ax.plot(dados["ano"], dados["gap_pct"],
            color=CORES[nivel], linewidth=2 if nivel == "Maranhão" else 1.5,
            linestyle="-" if nivel == "Maranhão" else "--",
            marker="o" if nivel == "Maranhão" else "s",
            markersize=5 if nivel == "Maranhão" else 4,
            label=nivel)

ax.axhline(100, color="gray", linestyle=":", linewidth=1, label="Paridade (100%)")
ax.set_xlabel("Ano")
ax.set_ylabel("Remuneração feminina / masculina (%)")
ax.set_title("Gap de gênero na remuneração dos economistas (2006–2022)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "04_gap_genero.png")
plt.close()

# =============================================================================
# BLOCO 5 — DISTRIBUIÇÃO POR CBO (último ano disponível)
# =============================================================================
print("Bloco 5: Distribuição por CBO...")

ano_max = df["ano"].max()
cbo_dist = (
    df[df["ano"] == ano_max]
    .groupby(["nivel_geografico", "cbo_descricao"])
    .size()
    .reset_index(name="n")
)
total_cbo = cbo_dist.groupby("nivel_geografico")["n"].transform("sum")
cbo_dist["pct"] = (cbo_dist["n"] / total_cbo * 100).round(1)
cbo_dist.to_csv(DIR_TABELAS / f"05_distribuicao_cbo_{ano_max}.csv", index=False)

fig, axes = plt.subplots(1, 3, figsize=(14, 6), sharey=True)
for ax, nivel in zip(axes, ORDEM_GEO):
    dados = (
        cbo_dist[cbo_dist["nivel_geografico"] == nivel]
        .sort_values("pct", ascending=True)
    )
    bars = ax.barh(dados["cbo_descricao"], dados["pct"],
                   color=CORES[nivel], alpha=0.85)
    ax.set_title(nivel, color=CORES[nivel], fontweight="bold")
    ax.set_xlabel("Participação (%)")
    for bar, val in zip(bars, dados["pct"]):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=7)

fig.suptitle(f"Distribuição por especialização CBO — {ano_max}", fontsize=12)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"05_distribuicao_cbo_{ano_max}.png")
plt.close()

# =============================================================================
# BLOCO 6 — DISTRIBUIÇÃO POR SETOR PÚBLICO / PRIVADO
# =============================================================================
print("Bloco 6: Setor público vs. privado...")

setor = (
    df.groupby(["ano", "nivel_geografico", "setor"])
    .size()
    .reset_index(name="n")
)
total_setor = setor.groupby(["ano", "nivel_geografico"])["n"].transform("sum")
setor["pct"] = setor["n"] / total_setor * 100

setor_pub = (
    setor[setor["setor"] == "Público"]
    .pivot(index="ano", columns="nivel_geografico", values="pct")[ORDEM_GEO]
    .round(1)
)
setor_pub.to_csv(DIR_TABELAS / "06_setor_publico_pct.csv")

fig, ax = plt.subplots(figsize=(10, 5))
for nivel in ORDEM_GEO:
    ax.plot(setor_pub.index, setor_pub[nivel],
            color=CORES[nivel], linewidth=2 if nivel == "Maranhão" else 1.5,
            linestyle="-" if nivel == "Maranhão" else "--",
            marker="o" if nivel == "Maranhão" else "s",
            markersize=5 if nivel == "Maranhão" else 4,
            label=nivel)

ax.set_xlabel("Ano")
ax.set_ylabel("Participação no setor público (%)")
ax.set_title("Proporção de economistas no setor público (2006–2022)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "06_setor_publico.png")
plt.close()

# =============================================================================
# BLOCO 7 — GRAU DE INSTRUÇÃO (Maranhão, último ano)
# =============================================================================
print("Bloco 7: Grau de instrução...")

ORDEM_INSTRUCAO = [
    "Superior completo", "Mestrado", "Doutorado",
    "Superior incompleto", "Médio completo", "Médio incompleto",
    "Fundamental completo", "6ª a 9ª fundamental",
    "5ª completo fundamental", "Até 5ª incompleto", "Analfabeto",
]

instrucao = (
    df[(df["nivel_geografico"] == "Maranhão") & (df["ano"] == ano_max)]
    .groupby("grau_instrucao")
    .size()
    .reset_index(name="n")
)
instrucao["pct"] = (instrucao["n"] / instrucao["n"].sum() * 100).round(1)
instrucao = instrucao.set_index("grau_instrucao").reindex(
    [i for i in ORDEM_INSTRUCAO if i in instrucao.index]
).reset_index()
instrucao.to_csv(DIR_TABELAS / f"07_instrucao_MA_{ano_max}.csv", index=False)

fig, ax = plt.subplots(figsize=(8, 5))
ax.barh(instrucao["grau_instrucao"], instrucao["pct"],
        color=CORES["Maranhão"], alpha=0.85)
for i, (_, row) in enumerate(instrucao.iterrows()):
    ax.text(row["pct"] + 0.2, i, f"{row['pct']:.1f}%", va="center", fontsize=8)
ax.set_xlabel("Participação (%)")
ax.set_title(f"Grau de instrução — economistas no Maranhão ({ano_max})")
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / f"07_instrucao_MA_{ano_max}.png")
plt.close()

# =============================================================================
# BLOCO 8 — PARTICIPAÇÃO DO MA NO NORDESTE
# =============================================================================
print("Bloco 8: Participação do MA no Nordeste...")

nordeste_total = (
    df[df["sigla_uf"].isin({"AL","BA","CE","MA","PB","PE","PI","RN","SE"})]
    .groupby(["ano", "sigla_uf"])
    .size()
    .reset_index(name="n")
)
total_ne = nordeste_total.groupby("ano")["n"].transform("sum")
nordeste_total["pct"] = (nordeste_total["n"] / total_ne * 100).round(2)

part_ma = nordeste_total[nordeste_total["sigla_uf"] == "MA"][["ano", "n", "pct"]]
part_ma.to_csv(DIR_TABELAS / "08_participacao_MA_nordeste.csv", index=False)

fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()
ax1.bar(part_ma["ano"], part_ma["n"], color=CORES["Maranhão"], alpha=0.7, label="Vínculos (MA)")
ax2.plot(part_ma["ano"], part_ma["pct"], color="black", linewidth=2,
         marker="o", markersize=5, label="% no Nordeste")
ax1.set_xlabel("Ano")
ax1.set_ylabel("Vínculos — Maranhão", color=CORES["Maranhão"])
ax2.set_ylabel("Participação no Nordeste (%)", color="black")
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax1.set_title("Maranhão: estoque e participação no Nordeste (2006–2022)")
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)
plt.tight_layout()
plt.savefig(DIR_GRAFICOS / "08_participacao_MA_nordeste.png")
plt.close()

# =============================================================================
# RELATÓRIO FINAL
# =============================================================================
print("\n" + "=" * 55)
print("ANÁLISE DESCRITIVA CONCLUÍDA")
print("=" * 55)
print(f"Tabelas  : {DIR_TABELAS}")
print(f"Gráficos : {DIR_GRAFICOS}")
print("\nArquivos gerados:")
for f in sorted(DIR_TABELAS.glob("*.csv")):
    print(f"  {f.name}")
for f in sorted(DIR_GRAFICOS.glob("*.png")):
    print(f"  {f.name}")
