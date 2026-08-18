library(sparklyr)
library(dplyr)

# Fuzzy matching distribuído por partição
fuzzy_match_spark <- function(df, coluna_nome, candidatos_df, sc, threshold = 88) {
  candidatos_local <- candidatos_df %>% collect()
  candidatos_map <- setNames(candidatos_local$cnpj, candidatos_local$nome_normalizado)
  
  df_matched <- spark_apply(
    df,
    function(pdf, context) {
      library(stringdist)
      
      c_map <- context$candidatos_map
      c_nomes <- names(c_map)
      
      cnpjs <- character(nrow(pdf))
      scores <- integer(nrow(pdf))
      
      for (i in seq_len(nrow(pdf))) {
        val <- pdf[[context$coluna_nome]][i]
        nome_norm <- if (is.na(val)) "" else toupper(trimws(val))
        nome_norm <- iconv(nome_norm, to = "ASCII//TRANSLIT")
        nome_norm <- gsub("[^A-Z0-9 ]", " ", nome_norm)
        nome_norm <- trimws(gsub("\\s+", " ", nome_norm))
        
        if (nome_norm == "" || length(c_nomes) == 0) {
          cnpjs[i] <- NA_character_
          scores[i] <- NA_integer_
          next
        }
        
        dists <- stringdist::stringdist(nome_norm, c_nomes, method = "token_sort")
        max_lens <- pmax(nchar(nome_norm), nchar(c_nomes))
        sims <- round((1 - (dists / max_lens)) * 100)
        
        best_idx <- which.max(sims)
        best_score <- sims[best_idx]
        
        if (length(best_score) > 0 && !is.na(best_score) && best_score >= context$threshold) {
          cnpjs[i] <- c_map[[c_nomes[best_idx]]]
          scores[i] <- as.integer(best_score)
        } else {
          cnpjs[i] <- NA_character_
          scores[i] <- NA_integer_
        }
      }
      
      pdf$cnpj_fuzzy <- cnpjs
      pdf$fuzzy_score <- scores
      return(pdf)
    },
    context = list(
      candidatos_map = candidatos_map,
      coluna_nome = coluna_nome,
      threshold = threshold
    )
  )
  
  return(df_matched)
}
