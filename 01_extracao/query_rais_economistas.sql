-- =============================================================================
-- EXTRAÇÃO RAIS - ECONOMISTAS, CONTADORES E ADMINISTRADORES
-- Fonte: Base dos Dados (basedosdados.org) via Google BigQuery
-- Tabela: basedosdados.br_me_rais.microdados_vinculos
-- CBO: 2512 (Economistas e afins)
-- Cobertura: 2006–2022 (CBO 2002 disponível a partir de 2003; 2006 em diante
--            garante cobertura completa com CNAE 2.0 e variáveis padronizadas)
-- Filtro:    vínculos ativos em 31/12 do ano de referência
-- =============================================================================

SELECT
  -- Identificação temporal e geográfica
  ano,
  sigla_uf,
  id_municipio,                        -- código IBGE 7 dígitos

  -- Ocupação
  cbo_2002,                            -- código CBO com 6 dígitos

  -- Atributos do trabalhador
  sexo,                                -- 1 = Masculino, 2 = Feminino
  faixa_etaria,
  grau_instrucao_apos_2005,
  raca_cor,
  nacionalidade,

  -- Atributos do vínculo
  vinculo_ativo_3112,                  -- 1 = ativo em 31/12
  tipo_vinculo,
  natureza_juridica,                   -- distinção público/privado
  tamanho_estabelecimento,

  -- Remuneração
  vl_remun_media_nom,                  -- remuneração média nominal (R$)
  vl_remun_media_sm,                   -- remuneração média em salários mínimos
  vl_remun_dezembro_nom,               -- remuneração em dezembro (nominal)

  -- Setor de atividade
  cnae_2,                              -- CNAE 2.0 nível de subclasse

  -- Jornada
  quantidade_horas_contratadas

FROM `basedosdados.br_me_rais.microdados_vinculos`

WHERE
  -- Vínculos ativos em 31 de dezembro do ano de referência
  vinculo_ativo_3112 = 1

  -- Família CBO 2512 — Economistas e afins
  AND SUBSTR(cbo_2002, 1, 4) = '2512'

  -- Janela temporal: 2006 a 2022
  -- (ajuste o limite superior conforme o ano mais recente disponível na BD)
  AND ano BETWEEN 2006 AND 2022

-- Ordenação para facilitar inspeção e particionamento local
ORDER BY ano, sigla_uf, cbo_2002

-- =============================================================================
-- NOTA SOBRE VOLUME E CUSTO:
-- A tabela completa da RAIS tem >400 GB. Com os filtros acima (família 2512
-- + vinculo_ativo_3112 = 1), o volume processado estimado é de ~200–500 MB
-- por ano, totalizando ~3–8 GB para a série completa — bem abaixo do limite
-- gratuito de 1 TB/mês do BigQuery.
-- =============================================================================
