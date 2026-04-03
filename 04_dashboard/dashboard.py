"""
DASHBOARD — ECONOMISTAS NO MARANHÃO
CORECON-MA | Fonte: RAIS 2006–2022
Framework: Streamlit
Deploy: Streamlit Community Cloud (gratuito)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ── CONFIGURAÇÃO DA PÁGINA ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Economistas no Maranhão | CORECON-MA",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── ESTILO ────────────────────────────────────────────────────────────────────
CORES = {
    "Maranhão": "#1a6b9a",
    "Nordeste":  "#e07b39",
    "Brasil":    "#4caf7d",
    "Público":   "#1a6b9a",
    "Privado":   "#e07b39",
    "Feminino":  "#e07b39",
    "Masculino": "#1a6b9a",
}

# ── CARREGAMENTO DE DADOS ─────────────────────────────────────────────────────
@st.cache_data
def carregar_dados():
    caminho = Path("data/processed/rais_economistas_clean.parquet")
    return pd.read_parquet(caminho)

df = carregar_dados()

FAIXA_ETARIA = {
    "1": "10 a 14", "2": "15 a 17", "3": "18 a 24", "4": "25 a 29",
    "5": "30 a 39", "6": "40 a 49", "7": "50 a 64", "8": "65 ou mais",
}
df["faixa_etaria_desc"] = df["faixa_etaria"].map(FAIXA_ETARIA).fillna("Não informado")

MUNICIPIOS_MA = {
    "2111300": "São Luís",       "2105302": "Imperatriz",
    "2111706": "São José de Ribamar", "2109700": "Paço do Lumiar",
    "2101400": "Açailândia",     "2112209": "Timon",
    "2104701": "Caxias",         "2103000": "Bacabal",
    "2107803": "Codó",           "2106805": "Balsas",
    "2108405": "Santa Inês",     "2109403": "Pinheiro",
}

ANOS      = sorted(df["ano"].unique())
ANO_MIN   = int(min(ANOS))
ANO_MAX   = int(max(ANOS))
ORDEM_GEO = ["Maranhão", "Nordeste", "Brasil"]

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/4/45/"
             "Bras%C3%A3o_do_Maranh%C3%A3o.svg/200px-Bras%C3%A3o_do_Maranh%C3%A3o.svg.png",
             width=80)
    st.title("Filtros")

    periodo = st.slider(
        "Período",
        min_value=ANO_MIN,
        max_value=ANO_MAX,
        value=(ANO_MIN, ANO_MAX),
        step=1,
    )

    sexo_sel = st.multiselect(
        "Sexo",
        options=["Feminino", "Masculino"],
        default=["Feminino", "Masculino"],
    )

    setor_sel = st.multiselect(
        "Setor",
        options=["Público", "Privado"],
        default=["Público", "Privado"],
    )

    cbo_opcoes = sorted(df["cbo_descricao"].unique())
    cbo_sel = st.multiselect(
        "Especialização (CBO)",
        options=cbo_opcoes,
        default=cbo_opcoes,
    )

    st.markdown("---")
    st.caption("Fonte: RAIS/MTE via Base dos Dados")
    st.caption("Elaboração: CORECON-MA")

# ── FILTRO GLOBAL ─────────────────────────────────────────────────────────────
mask = (
    (df["ano"] >= periodo[0]) &
    (df["ano"] <= periodo[1]) &
    (df["sexo"].isin(sexo_sel)) &
    (df["setor"].isin(setor_sel)) &
    (df["cbo_descricao"].isin(cbo_sel))
)
df_f  = df[mask]
df_ma = df_f[df_f["nivel_geografico"] == "Maranhão"]

# ── CABEÇALHO ─────────────────────────────────────────────────────────────────
st.title("📊 Economistas no Maranhão")
st.markdown(
    "Perfil do emprego formal dos economistas no Maranhão, "
    "com comparação ao Nordeste e Brasil. Fonte: RAIS 2006–2022."
)
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────
df_ma_ultimo = df_ma[df_ma["ano"] == min(ANO_MAX, periodo[1])]

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Vínculos no MA", f"{len(df_ma_ultimo):,}")
with col2:
    pct_fem = (
        (df_ma_ultimo["sexo"] == "Feminino").sum() / len(df_ma_ultimo) * 100
        if len(df_ma_ultimo) > 0 else 0
    )
    st.metric("Participação feminina", f"{pct_fem:.1f}%")
with col3:
    remun_media = df_ma_ultimo["valor_remuneracao_media"].mean()
    st.metric("Remuneração média (MA)", f"R$ {remun_media:,.0f}")
with col4:
    pct_pub = (
        (df_ma_ultimo["setor"] == "Público").sum() / len(df_ma_ultimo) * 100
        if len(df_ma_ultimo) > 0 else 0
    )
    st.metric("No setor público", f"{pct_pub:.1f}%")
with col5:
    ne_ultimo = df_f[
        (df_f["sigla_uf"].isin({"AL","BA","CE","MA","PB","PE","PI","RN","SE"})) &
        (df_f["ano"] == min(ANO_MAX, periodo[1]))
    ]
    pct_ne = (
        len(df_ma_ultimo) / len(ne_ultimo) * 100
        if len(ne_ultimo) > 0 else 0
    )
    st.metric("% no Nordeste", f"{pct_ne:.1f}%")

st.divider()

# ── ABAS ──────────────────────────────────────────────────────────────────────
aba1, aba2, aba3, aba4, aba5 = st.tabs([
    "📈 Evolução",
    "⚧ Gênero",
    "💰 Remuneração",
    "🏛️ Setor",
    "🗺️ Geografia",
])

# ── ABA 1: EVOLUÇÃO DO ESTOQUE ────────────────────────────────────────────────
with aba1:
    st.subheader("Evolução do estoque de economistas formais")

    estoque = (
        df_f.groupby(["ano", "nivel_geografico"])
        .size()
        .reset_index(name="n")
    )

    col_a, col_b = st.columns(2)

    with col_a:
        fig = px.line(
            estoque[estoque["nivel_geografico"] == "Maranhão"],
            x="ano", y="n",
            markers=True,
            title="Maranhão — vínculos ativos",
            color_discrete_sequence=[CORES["Maranhão"]],
        )
        fig.update_layout(xaxis_title="Ano", yaxis_title="Vínculos")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        fig = px.line(
            estoque[estoque["nivel_geografico"].isin(["Nordeste", "Brasil"])],
            x="ano", y="n",
            color="nivel_geografico",
            markers=True,
            title="Nordeste e Brasil — vínculos ativos",
            color_discrete_map=CORES,
        )
        fig.update_layout(xaxis_title="Ano", yaxis_title="Vínculos",
                          legend_title="")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Variação percentual acumulada")
    pivot = estoque.pivot(index="ano", columns="nivel_geografico", values="n")
    base = pivot.iloc[0]
    var_acum = ((pivot / base) - 1) * 100

    fig = go.Figure()
    for nivel in ORDEM_GEO:
        if nivel in var_acum.columns:
            fig.add_trace(go.Scatter(
                x=var_acum.index, y=var_acum[nivel],
                name=nivel, mode="lines+markers",
                line=dict(color=CORES[nivel],
                          width=3 if nivel == "Maranhão" else 1.5,
                          dash="solid" if nivel == "Maranhão" else "dash"),
            ))
    fig.update_layout(
        title="Variação acumulada do estoque (base = primeiro ano do período)",
        xaxis_title="Ano",
        yaxis_title="Variação (%)",
        yaxis_ticksuffix="%",
        legend_title="",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── ABA 2: GÊNERO ─────────────────────────────────────────────────────────────
with aba2:
    st.subheader("Composição e evolução por gênero")

    genero = (
        df_f.groupby(["ano", "nivel_geografico", "sexo"])
        .size()
        .reset_index(name="n")
    )
    total_gen = genero.groupby(["ano", "nivel_geografico"])["n"].transform("sum")
    genero["pct"] = genero["n"] / total_gen * 100

    fem = genero[genero["sexo"] == "Feminino"]

    fig = px.line(
        fem,
        x="ano", y="pct",
        color="nivel_geografico",
        markers=True,
        title="Razão de feminização (% feminino)",
        color_discrete_map=CORES,
    )
    fig.add_hline(y=50, line_dash="dot", line_color="gray",
                  annotation_text="Paridade (50%)")
    fig.update_layout(xaxis_title="Ano", yaxis_title="%",
                      yaxis_ticksuffix="%", legend_title="")
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader(f"Maranhão — {min(ANO_MAX, periodo[1])}")
        dist_sexo_ma = (
            df_ma_ultimo["sexo"].value_counts().reset_index()
        )
        dist_sexo_ma.columns = ["Sexo", "n"]
        fig = px.pie(dist_sexo_ma, names="Sexo", values="n",
                     color="Sexo", color_discrete_map=CORES,
                     hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Gap de remuneração por gênero")
        gap = (
            df_f.groupby(["ano", "nivel_geografico", "sexo"])
            ["valor_remuneracao_media"].mean().reset_index()
        )
        gap_pivot = gap.pivot_table(
            index=["ano", "nivel_geografico"],
            columns="sexo",
            values="valor_remuneracao_media"
        ).reset_index()
        if "Feminino" in gap_pivot.columns and "Masculino" in gap_pivot.columns:
            gap_pivot["gap_pct"] = (
                gap_pivot["Feminino"] / gap_pivot["Masculino"] * 100
            ).round(1)
            fig = px.line(
                gap_pivot,
                x="ano", y="gap_pct",
                color="nivel_geografico",
                markers=True,
                title="Remuneração feminina / masculina (%)",
                color_discrete_map=CORES,
            )
            fig.add_hline(y=100, line_dash="dot", line_color="gray",
                          annotation_text="Paridade (100%)")
            fig.update_layout(xaxis_title="Ano",
                              yaxis_title="%",
                              yaxis_ticksuffix="%",
                              legend_title="")
            st.plotly_chart(fig, use_container_width=True)

# ── ABA 3: REMUNERAÇÃO ────────────────────────────────────────────────────────
with aba3:
    st.subheader("Remuneração média dos economistas")

    remun = (
        df_f.groupby(["ano", "nivel_geografico"])
        ["valor_remuneracao_media"].mean().reset_index()
    )

    fig = px.line(
        remun,
        x="ano", y="valor_remuneracao_media",
        color="nivel_geografico",
        markers=True,
        title="Remuneração média nominal (R$)",
        color_discrete_map=CORES,
    )
    fig.update_layout(
        xaxis_title="Ano",
        yaxis_title="R$ (nominal)",
        yaxis_tickprefix="R$ ",
        yaxis_tickformat=",.0f",
        legend_title="",
    )
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Por especialização CBO — MA")
        remun_cbo = (
            df_ma[df_ma["ano"] == min(ANO_MAX, periodo[1])]
            .groupby("cbo_descricao")["valor_remuneracao_media"]
            .mean()
            .reset_index()
            .sort_values("valor_remuneracao_media", ascending=True)
        )
        fig = px.bar(
            remun_cbo,
            x="valor_remuneracao_media", y="cbo_descricao",
            orientation="h",
            title=f"Remuneração média por CBO — MA ({min(ANO_MAX, periodo[1])})",
            color_discrete_sequence=[CORES["Maranhão"]],
        )
        fig.update_layout(
            xaxis_title="R$ (nominal)", yaxis_title="",
            xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Distribuição salarial — MA")
        fig = px.box(
            df_ma[df_ma["ano"] == min(ANO_MAX, periodo[1])],
            x="sexo", y="valor_remuneracao_media",
            color="sexo",
            title=f"Distribuição da remuneração por sexo — MA ({min(ANO_MAX, periodo[1])})",
            color_discrete_map=CORES,
        )
        fig.update_layout(
            xaxis_title="", yaxis_title="R$ (nominal)",
            yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

# ── ABA 4: SETOR ──────────────────────────────────────────────────────────────
with aba4:
    st.subheader("Setor público e privado")

    setor_ev = (
        df_f.groupby(["ano", "nivel_geografico", "setor"])
        .size().reset_index(name="n")
    )
    total_s = setor_ev.groupby(["ano", "nivel_geografico"])["n"].transform("sum")
    setor_ev["pct"] = setor_ev["n"] / total_s * 100
    setor_pub = setor_ev[setor_ev["setor"] == "Público"]

    fig = px.line(
        setor_pub,
        x="ano", y="pct",
        color="nivel_geografico",
        markers=True,
        title="Proporção no setor público (%)",
        color_discrete_map=CORES,
    )
    fig.update_layout(xaxis_title="Ano", yaxis_title="%",
                      yaxis_ticksuffix="%", legend_title="")
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader(f"Remuneração por setor — MA ({min(ANO_MAX, periodo[1])})")
        remun_s = (
            df_ma[
                (df_ma["ano"] == min(ANO_MAX, periodo[1])) &
                (df_ma["setor"].isin(["Público", "Privado"]))
            ]
            .groupby("setor")["valor_remuneracao_media"]
            .agg(["mean", "median"])
            .reset_index()
        )
        remun_s.columns = ["Setor", "Média", "Mediana"]
        remun_s_melt = remun_s.melt(id_vars="Setor",
                                     var_name="Estatística",
                                     value_name="Valor")
        fig = px.bar(
            remun_s_melt,
            x="Setor", y="Valor",
            color="Estatística",
            barmode="group",
            color_discrete_sequence=["#1a6b9a", "#4caf7d"],
        )
        fig.update_layout(
            yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
            xaxis_title="", yaxis_title="R$ (nominal)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader(f"Composição por sexo e setor — MA ({min(ANO_MAX, periodo[1])})")
        ss = (
            df_ma[
                (df_ma["ano"] == min(ANO_MAX, periodo[1])) &
                (df_ma["setor"].isin(["Público", "Privado"]))
            ]
            .groupby(["setor", "sexo"]).size().reset_index(name="n")
        )
        total_ss = ss.groupby("setor")["n"].transform("sum")
        ss["pct"] = ss["n"] / total_ss * 100
        fig = px.bar(
            ss, x="setor", y="pct",
            color="sexo", barmode="group",
            color_discrete_map=CORES,
        )
        fig.update_layout(
            xaxis_title="", yaxis_title="%",
            yaxis_ticksuffix="%", legend_title="",
        )
        st.plotly_chart(fig, use_container_width=True)

# ── ABA 5: GEOGRAFIA ──────────────────────────────────────────────────────────
with aba5:
    st.subheader("Distribuição geográfica intraestadual — Maranhão")

    ano_geo = st.selectbox(
        "Selecione o ano", options=sorted(df_ma["ano"].unique(), reverse=True)
    )

    mun_dist = (
        df_ma[df_ma["ano"] == ano_geo]
        .groupby("id_municipio").size().reset_index(name="n")
    )
    mun_dist["nome"] = mun_dist["id_municipio"].map(MUNICIPIOS_MA).fillna(
        "Município " + mun_dist["id_municipio"]
    )
    mun_dist["pct"] = (mun_dist["n"] / mun_dist["n"].sum() * 100).round(1)
    mun_dist = mun_dist.sort_values("n", ascending=False).head(15)

    fig = px.bar(
        mun_dist.sort_values("n", ascending=True),
        x="n", y="nome", orientation="h",
        title=f"Top 15 municípios por vínculos ativos — {ano_geo}",
        color="n",
        color_continuous_scale=["#c6dff0", "#1a6b9a"],
        text="pct",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(
        xaxis_title="Vínculos", yaxis_title="",
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Evolução por UF — Nordeste")
    ne_uf = (
        df_f[df_f["sigla_uf"].isin({"AL","BA","CE","MA","PB","PE","PI","RN","SE"})]
        .groupby(["ano", "sigla_uf"]).size().reset_index(name="n")
    )
    fig = px.line(
        ne_uf, x="ano", y="n",
        color="sigla_uf",
        markers=True,
        title="Estoque de economistas por UF — Nordeste",
    )
    fig.update_layout(xaxis_title="Ano", yaxis_title="Vínculos",
                      legend_title="UF")
    st.plotly_chart(fig, use_container_width=True)

# ── RODAPÉ ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Conselho Regional de Economia — Maranhão (CORECON-MA) | "
    "Dados: RAIS/MTE via Base dos Dados | "
    "Período: 2006–2022 | Vínculos ativos em 31/12 | CBO família 2512"
)
