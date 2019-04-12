
#' Postggres Connection
#'
#' This function allows you to connect to postgres.
#' @keywords Postgres
#' @export
#' @examples
#' getTable()

getTable <- function() {
    connstring <- Sys.getenv('localingressstring')
    if (nchar(connstring) > 0) { # Detect local dev
      library('RPostgreSQL')
      conn <- connectToPostgres()
      tables <- dbGetQuery(conn, "SELECT * FROM tables_index")
      tables
    }
    else { # We're in spark
      library('SparkR')
      s <- read.df(source="json", path='/FileStore/tables/secrets.json')
      conn <- as.data.frame(s)
      connstring <- conn$ingressconnectionstring[1]
      df <- as.data.frame(read.jdbc(connstring, 'tables_index'))
      df
    }
}

connectToPostgres <- function() {
    connectionParams <- strsplit(connstring, "[|]")
    pg <- dbDriver("PostgreSQL")
    connection <- dbConnect(pg, connectionParams[[1]][1], user=connectionParams[[1]][2], password=connectionParams[[1]][3], dbname=connectionParams[[1]][4], port=5432)
    connection
}