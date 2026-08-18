# Leitura e escrita no PostgreSQL via JDBC
get_env <- function(var_name, default_val) {
  val <- Sys.getenv(var_name)
  if (val == "") default_val else val
}

DB_HOST <- get_env("DB_HOST", "postgres")
DB_PORT <- get_env("DB_PORT", "5432")
DB_USER <- get_env("DB_USER", "postgres")
DB_PASSWORD <- get_env("DB_PASSWORD", "postgres")
DB_NAME <- get_env("DB_NAME", "eedb")

JDBC_URL <- sprintf("jdbc:postgresql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME)

write_table_jdbc <- function(df, schema, table, mode = "overwrite") {
  sdf <- spark_dataframe(df)
  writer <- sdf %>% 
    invoke("write") %>% 
    invoke("format", "jdbc") %>% 
    invoke("option", "url", JDBC_URL) %>% 
    invoke("option", "dbtable", sprintf("%s.%s", schema, table)) %>% 
    invoke("option", "user", DB_USER) %>% 
    invoke("option", "password", DB_PASSWORD) %>% 
    invoke("option", "driver", "org.postgresql.Driver") %>% 
    invoke("mode", mode)
  
  writer %>% invoke("save")
}

read_table_jdbc <- function(sc, schema, table) {
  spark_read_jdbc(
    sc,
    name = sprintf("%s_%s", schema, table),
    options = list(
      url = JDBC_URL,
      dbtable = sprintf("%s.%s", schema, table),
      user = DB_USER,
      password = DB_PASSWORD,
      driver = "org.postgresql.Driver"
    ),
    memory = FALSE
  )
}
