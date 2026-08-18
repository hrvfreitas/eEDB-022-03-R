cat("\n=== [1/3] Executando Ingestão RAW ===\n")
source("01_ingest_raw.R")
run_ingest_raw()

cat("\n=== [2/3] Executando Camada Trusted ===\n")
source("02_trusted.R")
run_trusted()

cat("\n=== [3/3] Executando Camada Delivery ===\n")
source("03_delivery.R")
run_delivery()

cat("\nPipeline completo em R executado com sucesso!\n")
