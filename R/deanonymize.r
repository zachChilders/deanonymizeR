
#' GetTable
#'
#' Fetches a table from Postgres, returns as Dataframe
#' @export
#' @examples
#' getTable(table_name)
getTable <- function(table_name) {
  connstring <- Sys.getenv('localingressstring')
  if (nchar(connstring) > 0) { # Detect local dev
    library('RPostgreSQL')
    conn <- connectToPostgres(connstring)
    tables <- dbGetQuery(conn, paste("SELECT * FROM", table_name))
    tables
  }
  else { # We're in spark
    s <- read.df(source="json", path='/FileStore/tables/secrets.json')
    conn <- as.data.frame(s)
    connstring <- conn$ingressconnectionstring[1]
    df <- as.data.frame(read.jdbc(connstring, table_name))  
    as.data.frame(df)
  }
}


#' SetTable
#' 
#' Sets a row state in the metadata table
#' @export
#' @examples
#' setTable(table_row, state)
setTable <- function(table_row, state) {
  connstring <- Sys.getenv('localingressstring')
  if (nchar(connstring) > 0) { # Detect local dev
    library('RPostgreSQL')
    conn <- connectToPostgres(connstring)
    query <- paste("UPDATE tables_index SET state =", state, "WHERE id =", table_row$id)
    dbSendQuery(conn, query)
  }
}


#' ConnectToPostgres
#'
#' Local helper function to attach to Postgres. Must have run init.sh previously
#' @export
#' @examples
#' connectToPostgres(connstring)
connectToPostgres <- function(connstring) {
  connectionParams <- strsplit(connstring, "[|]")
  pg <- dbDriver("PostgreSQL")
  connection <- dbConnect(pg, connectionParams[[1]][1], user=connectionParams[[1]][2], password=connectionParams[[1]][3], dbname=connectionParams[[1]][4], port=5432)
  connection
}


#' Enum
#' 
#' Creates an Enum from given values
#' @export
#' @examples
#' Enum(...)
Enum <- function(...) {
  values <- sapply(match.call(expand.dots = TRUE)[-1L], deparse)
  
  stopifnot(identical(unique(values), values))

  res <- setNames(seq_along(values), values)
  res <- as.environment(as.list(res))
  lockEnvironment(res, bindings = TRUE)
  res
}


#' States
#'
#' An Enum that drives the FSM
#' @export
#' @examples
#' states
states <- Enum(SCRAPEINPROGRESS, SCRAPESUCCESS, SCRAPEFAIL, TRIAGEINPROGRESS, TRIAGESUCCESS, TRIAGEFAIL)