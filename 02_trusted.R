source("spark_session.R")
source("db.R")
source("fuzzy_match.R")

TRUSTED_OUT_DIR <- Sys.getenv("TRUSTED_OUT_DIR", "/opt/data/trusted")

tratar_bancos <- function(sc) {
  df <- read_table_jdbc(sc, "raw", "bancos")
  
  df_prep <- df %>% 
    mutate(
      cnpj = lpad(substring(regexp_replace(cast(CNPJ as string), "[^0-9]", ""), 1, 8), 8, "0"),
      nome_instituicao = trim(NomeInstituicao),
      segmento_prudencial = trim(Segmento),
      prioridade = if_else(upper(nome_instituicao) %like% "%PRUDENCIAL%", 0, 1)
    )
  
  principal <- df_prep %>% 
    group_by(cnpj) %>% 
    window_order(prioridade) %>% 
    mutate(rn = row_number()) %>% 
    filter(rn == 1) %>% 
    ungroup()
  
  alternativo <- df_prep %>% 
    group_by(cnpj) %>% 
    window_order(prioridade) %>% 
    mutate(rn = row_number()) %>% 
    filter(rn == 2) %>% 
    select(cnpj_alt = cnpj, nome_alternativo = nome_instituicao) %>% 
    ungroup()
  
  trusted_bancos <- principal %>% 
    left_join(alternativo, by = c("cnpj" = "cnpj_alt")) %>% 
    select(cnpj, nome_instituicao, segmento_prudencial, nome_alternativo) %>% 
    sdf_distinct(col = "cnpj")
  
  return(trusted_bancos)
}

run_trusted <- function() {
  sc <- get_spark_session("02_trusted")
  trusted_bancos <- tratar_bancos(sc)
  
  spark_write_parquet(trusted_bancos, file.path(TRUSTED_OUT_DIR, "bancos"), mode = "overwrite")
  write_table_jdbc(trusted_bancos, "trusted", "bancos")
  
  spark_disconnect(sc)
}
