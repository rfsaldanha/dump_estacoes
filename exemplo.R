# Exemplo de conexão no banco de dados
con <- DBI::dbConnect(duckdb::duckdb(), "estacoes.duckdb")

# Lista de tabels
DBI::dbListTables(con)

# Consulta
res <- dplyr::tbl(con, "tab_estacao_3") |>
  head(10) |>
  dplyr::collect()

res

# Desconecta do banco
DBI::dbDisconnect(con)
