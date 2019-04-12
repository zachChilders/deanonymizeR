
#' Postggres Connection
#'
#' This function allows you to connect to postgres.
#' @keywords Postgres
#' @export
#' @examples
#' connetToPostgres()
 
connectToPostgres <- function() {
    connstring <- Sys.getenv('localingressstring')
    if (nchar(connstring) > 0) { # Detect local dev
        library('RPostgreSQL')
        connectionParams <- strsplit(connstring, "[|]")
        pg <- dbDriver("PostgresSQL")
        connection <- dbConnect(pg, connectionParams[[1]][1], user=connectionParams[[1]][2], password=connectionParams[[1]][3], dbname=connectionParams[[1]][4], port=5432)

    }
    else { # We're in spark
        return 
    }
}