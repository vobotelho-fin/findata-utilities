library(httr)
library(jsonlite)
library(openssl)
library(dplyr)
library(DBI)
library(glue)
library(shinycssloaders)
library(future)
library(promises)
library(ggplot2)
library(officer)
library(lubridate)
library(ggrepel)
library(readxl)
library(readr)
library(tidyr)
library(stringr)
library(quarto)
library(archive)
library(data.table)
library(janitor)
library(foreach)
library(seasonal)
library(RMariaDB)
library(usethis)
library(gitcreds)
rm(list=ls())

dataset_connect <- function() {
  con <- dbConnect(
    RMariaDB::MariaDB(),
    dbname = keys[["dbname"]],
    host = keys[["host"]],
    user = keys[["user"]],
    password = keys[["password"]],
    port = 3306,
    client.flag = RMariaDB::CLIENT_LOCAL_FILES
  )
  return(con)
}

acum_tx <- function(data, col, mult, per) {
  aux <- (1 + data[, col] / mult)
  
  data[, ncol(data) + 1] <- NA
  for (i in per:NROW(data)) {
    data[i, ncol(data)] <- (prod(aux[(i-per+1):i]) - 1) * mult
  }
  names(data)[ncol(data)] <- paste0(colnames(data)[col], "_ac", per, "per")
  return(data)
}

acum <- function(data, col, col_data, per, acc) {
  data[, ncol(data) + 1] <- NA
  acc <- "month"
  
  for (i in 1:NROW(data)) {
    if (i < per) {
      dates <- seq(data[[i, col_data]], by = paste0("-1 ", acc), length.out = i)
    } else {
      dates <- seq(data[[i, col_data]], by = paste0("-1 ", acc), length.out = per)
    }
    data[i, ncol(data)] <- sum(subset(data, date_value %in% dates)[, col], na.rm = TRUE)
  }
  names(data)[ncol(data)] <- paste0(colnames(data)[col], "_ac", per, "per")
  return(data)
}

create_table <- function(name, data) {
  con <- dataset_connect()
  
  dbExecute(con, paste0("DROP TABLE IF EXISTS ", name, ";"))
  
  map_types <- function(x) {
    if (is.integer(x)) return("INT")
    if (is.numeric(x)) return("DOUBLE")
    if (inherits(x, "Date")) return("DATE")
    return("TEXT") # default
  }
  
  col_defs <- mapply(function(name_col, col) {
    sql_type <- map_types(col)
    if (name_col == "date_value") {
      paste0("`", name_col, "` ", sql_type, " PRIMARY KEY")
    } else {
      paste0("`", name_col, "` ", sql_type)
    }
  }, colnames(dados), dados)
  
  sql_create <- paste0(
    "CREATE TABLE IF NOT EXISTS ", name, " (",
    paste(col_defs, collapse = ", "),
    ")"
  )
  
  dbExecute(con, sql_create)
  
  dbDisconnect(con)
}

upload <- function(data, bd, id_interno) {
  con <- dataset_connect()
  
  existing_cols <- dbGetQuery(con, glue_sql("SHOW COLUMNS FROM {`bd`}", .con = con))$Field
  # Check if id_interno exists; if not, add it as a DOUBLE (you can adjust type)
  if (!(id_interno %in% existing_cols)) {
    alter_query <- glue_sql("ALTER TABLE {`bd`} ADD COLUMN {`id_interno`} DOUBLE", .con = con)
    dbExecute(con, alter_query)
  }
  
  for (i in 1:NROW(data)) {
    query <- glue_sql("
                      INSERT IGNORE INTO {`bd`} (date_value, {`id_interno`})
                      VALUES ({data$date_value[i]}, {data$value[i]})
                      ON DUPLICATE KEY UPDATE {`id_interno`} = VALUES({`id_interno`})
                      ", .con = con)
    
    dbExecute(con, query)
  }
  
  dbDisconnect(con)
}

column_exists <- function(bd, id_interno) {
  con <- dataset_connect()
  
  existing_cols <- dbGetQuery(con, glue_sql("SHOW COLUMNS FROM {`bd`}", .con = con))$Field
  # Check if id_interno exists; if not, add it as a DOUBLE (you can adjust type)
  if (!(id_interno %in% existing_cols)) {
    alter_query <- glue_sql("ALTER TABLE {`bd`} ADD COLUMN {`id_interno`} DOUBLE", .con = con)
    dbExecute(con, alter_query)
  }
  
  dbDisconnect(con)
}

extract_data <- function(id_interno, table, name) {
  con <- dataset_connect()
  
  query <- glue_sql("
                      SELECT date_value, {`id_interno`} FROM {`table`}
                      ", .con = con)
  
  result <- dbGetQuery(con, query) %>%
    rename(!!name := !!sym(id_interno)) %>%
    mutate(date_value = as.Date(date_value))
  
  dbDisconnect(con)
  return(result)
}

extract_microdata <- function(id_interno, table, name) {
  con <- dataset_connect()
  
  pk_query <- sprintf("
  SELECT column_name
  FROM information_schema.key_column_usage
  WHERE table_schema = DATABASE()
    AND table_name = '%s'
    AND constraint_name = 'PRIMARY';", table)
  
  pk_cols <- dbGetQuery(con, pk_query)$COLUMN_NAME
  
  query <- glue_sql("
                      SELECT {`pk_cols`}, {`id_interno`} FROM {`table`}
                      ", .con = con)
  
  result <- dbGetQuery(con, query) %>%
    rename(!!name := !!sym(id_interno)) %>%
    rename(!!"primary_key" := !!sym(pk_cols))
  
  dbDisconnect(con)
  return(result)
}

extract_microdata_free <- function(id_interno, table, name) {
  con <- dataset_connect()
  
  query <- glue_sql("SELECT {`id_interno`} FROM {`table`}", .con = con)  
  
  result <- dbGetQuery(con, query) %>% rename(!!name := !!sym(id_interno))
  
  dbDisconnect(con)
  return(result)
}