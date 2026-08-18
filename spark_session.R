# Inicialização da sessão Spark via sparklyr
library(sparklyr)
library(dplyr)

POSTGRES_JDBC_PACKAGE <- "org.postgresql:postgresql:42.7.4"

get_spark_session <- function(app_name = "R_Spark_Pipeline") {
  config <- spark_config()
  config$`sparklyr.shell.name` <- app_name
  config$`sparklyr.jars.packages` <- POSTGRES_JDBC_PACKAGE
  config$`spark.sql.session.timeZone` <- "UTC"
  
  spark_master <- Sys.getenv("SPARK_MASTER", "local[4]")
  
  sc <- spark_connect(master = spark_master, config = config)
  return(sc)
}
