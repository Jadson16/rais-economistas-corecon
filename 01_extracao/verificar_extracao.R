# =============================================================================
# VERIFICAÇÃO PÓS-EXTRAÇÃO
# Script: verificar_extracao.R
# Objetivo: validar integridade dos dados baixados antes de prosseguir
#           para o estágio de tratamento (clean_rais.R)
# =============================================================================

library(arrow)
library(dplyr)
library(tidyr)
library(ggplot2)

# --- 1. Leitura --------------------------------------------------------------

DIR_PROCESSED <- here::here("data", "processed")

df <- arrow::read_parquet(
  file.path(DIR_PROCESSED, "rais_economistas.parquet")
)

# --- 2. Verificações básicas -------------------------------------------------

cat("========================================\n")
cat("RELATÓRIO DE VERIFICAÇÃO - RAIS\n")
cat("========================================\n\n")

cat("Dimensões:\n")
cat("  Linhas  :", scales::comma(nrow(df)), "\n")
cat("  Colunas :", ncol(df), "\n\n")

cat("Cobertura temporal:\n")
print(table(df$ano))

cat("\nCódigos CBO presentes na extração:\n")
df |>
  count(cbo_2002, sort = TRUE) |>
  mutate(
    descricao = case_when(
      cbo_2002 == "251205" ~ "Economista",
      cbo_2002 == "251210" ~ "Economista agrícola",
      cbo_2002 == "251215" ~ "Economista industrial",
      cbo_2002 == "251220" ~ "Economista do trabalho",
      cbo_2002 == "251225" ~ "Economista do setor público",
      TRUE                 ~ "Outro/não mapeado"
    ),
    pct = scales::percent(n / sum(n), accuracy = 0.1)
  ) |>
  print()

cat("\nDistribuição por sexo:\n")
df |>
  count(sexo) |>
  mutate(pct = scales::percent(n / sum(n), accuracy = 0.1)) |>
  print()

cat("\nDistribuição por UF (top 10):\n")
df |>
  count(sigla_uf, sort = TRUE) |>
  slice_head(n = 10) |>
  print()

cat("\nMissings por variável:\n")
df |>
  summarise(across(everything(), ~ sum(is.na(.)))) |>
  tidyr::pivot_longer(everything(),
                      names_to  = "variavel",
                      values_to = "n_missing") |>
  filter(n_missing > 0) |>
  mutate(pct_missing = scales::percent(n_missing / nrow(df), accuracy = 0.01)) |>
  print()

cat("\nEstatísticas de remuneração (vínculos com remun > 0):\n")
df |>
  filter(vl_remun_media_nom > 0) |>
  summarise(
    min    = min(vl_remun_media_nom, na.rm = TRUE),
    p25    = quantile(vl_remun_media_nom, 0.25, na.rm = TRUE),
    mediana = median(vl_remun_media_nom, na.rm = TRUE),
    media  = mean(vl_remun_media_nom, na.rm = TRUE),
    p75    = quantile(vl_remun_media_nom, 0.75, na.rm = TRUE),
    max    = max(vl_remun_media_nom, na.rm = TRUE)
  ) |>
  mutate(across(everything(), scales::comma)) |>
  print()

cat("\n========================================\n")
cat("Verificação concluída. Se os totais e\n")
cat("distribuições parecerem razoáveis,\n")
cat("prossiga para clean_rais.R\n")
cat("========================================\n")
