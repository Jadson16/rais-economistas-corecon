# =============================================================================
# DOWNLOAD RAIS VIA BASE DOS DADOS (Google BigQuery)
# Script: download_bigquery.R
# Requer: pacotes basedosdados, arrow, dplyr, purrr
#
# PRÉ-REQUISITOS:
#   1. Conta Google e projeto criado em console.cloud.google.com (gratuito)
#   2. Copiar o ID do projeto (ex: "meu-projeto-rais-123") para BILLING_PROJECT
#   3. Na primeira execução, o pacote abrirá o navegador para autenticação OAuth
# =============================================================================

# --- 0. Instalação de pacotes (executar apenas uma vez) ----------------------

# install.packages(c("basedosdados", "arrow", "dplyr", "purrr", "fs"))

library(basedosdados)
library(arrow)
library(dplyr)
library(purrr)
library(fs)

# --- 1. Configuração ---------------------------------------------------------

# ATENÇÃO: substitua pelo ID do seu projeto no Google Cloud
BILLING_PROJECT <- "rais-economistas"

# Diretório de saída (Parquet particionado por ano)
DIR_RAW <- here::here("data", "raw", "rais_cbo")
fs::dir_create(DIR_RAW)

# Família CBO de interesse
CBO_FAMILIAS <- "2512"

# Janela temporal
ANOS <- 2006:2022

# --- 2. Função de extração por ano ------------------------------------------
# A extração ano a ano evita timeout, facilita reprocessamento incremental
# e mantém controle sobre o volume processado por sessão do BigQuery.

extrair_ano <- function(ano, billing_project, dir_saida) {

  arquivo_saida <- file.path(dir_saida, paste0("rais_cbo_", ano, ".parquet"))

  # Pula anos já baixados (retomada em caso de interrupção)
  if (fs::file_exists(arquivo_saida)) {
    message(paste0("Ano ", ano, ": arquivo já existe, pulando."))
    return(invisible(NULL))
  }

  message(paste0("Extraindo ano ", ano, "..."))

  # Usa sprintf para interpolar apenas o ano, evitando conflito entre
  # glue e as chaves presentes em funções SQL como SUBSTR()
  query <- sprintf("
    SELECT
      ano,
      sigla_uf,
      id_municipio,
      cbo_2002,
      sexo,
      faixa_etaria,
      grau_instrucao_apos_2005,
      raca_cor,
      nacionalidade,
      vinculo_ativo_3112,
      tipo_vinculo,
      natureza_juridica,
      tamanho_estabelecimento,
      vl_remun_media_nom,
      vl_remun_media_sm,
      vl_remun_dezembro_nom,
      cnae_2,
      quantidade_horas_contratadas
    FROM `basedosdados.br_me_rais.microdados_vinculos`
    WHERE
      ano = %d
      AND vinculo_ativo_3112 = 1
      AND SUBSTR(cbo_2002, 1, 4) = '2512'
  ", ano)

  tryCatch({
    df <- basedosdados::read_sql(
      query              = query,
      billing_project_id = billing_project
    )

    arrow::write_parquet(df, arquivo_saida)
    message(paste0("  -> ", nrow(df), " vínculos salvos em ", arquivo_saida))

  }, error = function(e) {
    message(paste0("  ERRO no ano ", ano, ": ", e$message))
  })
}

# --- 3. Execução sequencial --------------------------------------------------

# Na primeira execução, o navegador será aberto para autenticação Google OAuth.
# Após autenticar uma vez, as credenciais ficam em cache local.

basedosdados::set_billing_id(BILLING_PROJECT)

purrr::walk(ANOS, extrair_ano,
            billing_project = BILLING_PROJECT,
            dir_saida       = DIR_RAW)

message("Download concluído. Arquivos em: ", DIR_RAW)

# --- 4. Consolidação em dataset único ----------------------------------------
# Após o download de todos os anos, consolida em um único Parquet particionado.

consolidar_parquet <- function(dir_raw, dir_processado) {

  fs::dir_create(dir_processado)

  arquivos <- fs::dir_ls(dir_raw, glob = "*.parquet")

  message(paste0("Consolidando ", length(arquivos), " arquivos..."))

  df_consolidado <- purrr::map_dfr(arquivos, arrow::read_parquet)

  arrow::write_parquet(
    df_consolidado,
    file.path(dir_processado, "rais_economistas.parquet")
  )

  message("Consolidação concluída.")
  message(paste0("  Total de vínculos: ", scales::comma(nrow(df_consolidado))))
  message(paste0("  Período: ", min(df_consolidado$ano), "–", max(df_consolidado$ano)))
}

DIR_PROCESSED <- here::here("data", "processed")

consolidar_parquet(DIR_RAW, DIR_PROCESSED)

# =============================================================================
# ALTERNATIVA SEM GOOGLE CLOUD:
# Caso prefira baixar os microdados brutos diretamente do MTE (ftp.mtps.gov.br),
# use o script download_ftp_mte.R (a ser desenvolvido no Estágio 2).
# Os arquivos brutos são .7z com layout fixo; o volume é maior e o processo
# de leitura exige o pacote {vroom} ou {data.table} + dicionário de colunas.
# A via Base dos Dados é significativamente mais eficiente para esta análise.
# =============================================================================

library(bigrquery)

# Evita SUBSTR() usando LIKE — contorna o bug do cli
query_teste <- "
  SELECT ano, sigla_uf, cbo_2002, sexo, COUNT(*) as n
  FROM `basedosdados.br_me_rais.microdados_vinculos`
  WHERE ano = 2022
    AND vinculo_ativo_3112 = 1
    AND cbo_2002 LIKE '2512%'
  GROUP BY ano, sigla_uf, cbo_2002, sexo
"

tb <- bigrquery::bq_project_query(BILLING_PROJECT, query_teste)
df <- bigrquery::bq_table_download(tb)
print(head(df))
