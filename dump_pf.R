# Packages
library(DBI)
library(RPostgres)
library(duckdb)
library(dbplyr)
library(dplyr)
library(lubridate)
library(glue)

# Database connection
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "observatorio",
  host = "psql.icict.fiocruz.br",
  port = 5432,
  user = Sys.getenv("weather_user"),
  password = Sys.getenv("weather_password")
)

con_duck <- dbConnect(duckdb(), "estacoes.duckdb")

# Plugfield
## Schema
schema_estacao_1b <- dbplyr::in_schema("estacoes", "tb_estacao_1b")
schema_estacao_3 <- dbplyr::in_schema("estacoes", "tb_estacao_3")
schema_estacao_4 <- dbplyr::in_schema("estacoes", "tb_estacao_4")

## Table
tab_estacao_1b <- tbl(con, schema_estacao_1b)
tab_estacao_3 <- tbl(con, schema_estacao_3)
tab_estacao_4 <- tbl(con, schema_estacao_4)

## Get data
dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_1b",
  value = tab_estacao_1b |> collect(),
  overwrite = TRUE
)

dbWriteTable(
  conn = con_duck,
  name = "tab_estacao_3",
  value = tab_estacao_3 |> collect(),
  overwrite = TRUE
)

dbWriteTable(
  conn = con_duck,
  name = "tab_estacao_4",
  value = tab_estacao_4 |> collect(),
  overwrite = TRUE
)

dbDisconnect(conn = con)

dbListTables(con_duck)

dbDisconnect(conn = con_duck)
