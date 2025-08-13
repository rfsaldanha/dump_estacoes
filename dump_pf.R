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
schema <- dbplyr::in_schema("estacoes", "tb_estacao_1b")

## Table
tab <- tbl(con, schema)

## Get data
dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_1b",
  value = tab |> collect()
)

dbDisconnect(conn = con)
dbDisconnect(conn = con_duck)
