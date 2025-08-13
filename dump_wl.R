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

# Weather link
## Schemas
schema_sensor_772002 <- dbplyr::in_schema(
  "estacoes",
  "tb_estacao_2_sensor_772002"
)
schema_sensor_772003 <- dbplyr::in_schema(
  "estacoes",
  "tb_estacao_2_sensor_772003"
)
schema_sensor_772004 <- dbplyr::in_schema(
  "estacoes",
  "tb_estacao_2_sensor_772004"
)
schema_sensor_772005 <- dbplyr::in_schema(
  "estacoes",
  "tb_estacao_2_sensor_772005"
)

## Tables
tab_sensor_772002 <- tbl(con, schema_sensor_772002)
tab_sensor_772003 <- tbl(con, schema_sensor_772003)
tab_sensor_772004 <- tbl(con, schema_sensor_772004)
tab_sensor_772005 <- tbl(con, schema_sensor_772005)

## Get data
dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_2_sensor_772002",
  value = tab_sensor_772002 |> collect()
)

dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_2_sensor_772003",
  value = tab_sensor_772003 |> collect()
)
dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_2_sensor_772004",
  value = tab_sensor_772004 |> collect()
)

dbWriteTable(
  conn = con_duck,
  name = "tb_estacao_2_sensor_772005",
  value = tab_sensor_772005 |> collect()
)

dbDisconnect(conn = con)
dbDisconnect(conn = con_duck)
