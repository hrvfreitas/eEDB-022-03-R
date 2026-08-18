source("spark_session.R")
source("db.R")

DELIVERY_OUT_DIR <- Sys.getenv("DELIVERY_OUT_DIR", "/opt/data/delivery")

run_delivery <- function() {
  sc <- get_spark_session("03_delivery")
  
  bancos <- read_table_jdbc(sc, "trusted", "bancos")
  reclamacoes <- read_table_jdbc(sc, "trusted", "reclamacoes")
  empregados <- read_table_jdbc(sc, "trusted", "empregados")
  
  delivery <- reclamacoes %>% 
    filter(!is.na(cnpj)) %>% 
    inner_join(bancos, by = "cnpj", suffix = c("_rec", "_banco")) %>% 
    left_join(empregados, by = "cnpj", suffix = c("", "_emp")) %>% 
    mutate(possui_avaliacao_glassdoor = if_else(!is.na(origem_arquivo), TRUE, FALSE))
  
  spark_write_parquet(delivery, file.path(DELIVERY_OUT_DIR, "tb_reclamacoes_bancos_funcionarios"), mode = "overwrite")
  write_table_jdbc(delivery, "delivery", "tb_reclamacoes_bancos_funcionarios")
  
  spark_disconnect(sc)
}
