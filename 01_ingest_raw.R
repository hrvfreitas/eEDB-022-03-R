source("spark_session.R")
source("db.R")

RAW_ORIGEM_DIR <- Sys.getenv("RAW_ORIGEM_DIR", "/opt/data/raw/origem")
RAW_OUT_DIR <- Sys.getenv("RAW_OUT_DIR", "/opt/data/raw")

run_ingest_raw <- function() {
  sc <- get_spark_session("01_ingest_raw")
  
  df_reclamacoes <- spark_read_csv(
    sc, name = "raw_reclamacoes_tmp",
    path = file.path(RAW_ORIGEM_DIR, "Reclamações", "*.csv"),
    delimiter = ";", charset = "ISO-8859-1", header = TRUE, infer_schema = FALSE
  ) %>% mutate(arquivo_origem = input_file_name())
  
  df_bancos <- spark_read_csv(
    sc, name = "raw_bancos_tmp",
    path = file.path(RAW_ORIGEM_DIR, "Bancos", "EnquadramentoInicia_v2.tsv"),
    delimiter = "\t", charset = "ISO-8859-1", header = TRUE, infer_schema = FALSE
  )
  
  base_emp <- file.path(RAW_ORIGEM_DIR, "Empregados")
  df_emp_match <- spark_read_csv(
    sc, name = "raw_emp_match_tmp", path = file.path(base_emp, "glassdoor_consolidado_join_match_v2.csv"),
    delimiter = "|", charset = "UTF-8", header = TRUE, infer_schema = FALSE
  ) %>% mutate(arquivo_origem = "match")
  
  df_emp_match_less <- spark_read_csv(
    sc, name = "raw_emp_match_less_tmp", path = file.path(base_emp, "glassdoor_consolidado_join_match_less_v2.csv"),
    delimiter = "|", charset = "UTF-8", header = TRUE, infer_schema = FALSE
  ) %>% mutate(arquivo_origem = "match_less")
  
  # Parquet e Postgres
  spark_write_parquet(df_reclamacoes, file.path(RAW_OUT_DIR, "reclamacoes"), mode = "overwrite")
  spark_write_parquet(df_bancos, file.path(RAW_OUT_DIR, "bancos"), mode = "overwrite")
  spark_write_parquet(df_emp_match, file.path(RAW_OUT_DIR, "empregados_match"), mode = "overwrite")
  spark_write_parquet(df_emp_match_less, file.path(RAW_OUT_DIR, "empregados_match_less"), mode = "overwrite")
  
  write_table_jdbc(df_reclamacoes, "raw", "reclamacoes")
  write_table_jdbc(df_bancos, "raw", "bancos")
  write_table_jdbc(df_emp_match, "raw", "empregados_match")
  write_table_jdbc(df_emp_match_less, "raw", "empregados_match_less")
  
  spark_disconnect(sc)
}
