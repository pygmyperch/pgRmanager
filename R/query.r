#' Execute a SQL query
#'
#' This function executes a given SQL query on the connected database.
#'
#' @param query A string containing the SQL query to execute
#' @param params A list of parameters to be used in the query (for parameterized queries)
#' @param fetch Logical, if TRUE (default) fetches and returns the results
#'
#' @return If fetch is TRUE, returns a data frame with the query results. Otherwise, returns NULL.
#'
#' @importFrom DBI dbGetQuery dbExecute
#' @export
execute_query <- function(query, params = NULL, fetch = TRUE) {
  conn <- get_connection()
  if (is.null(conn)) {
    stop("No active database connection. Use connect_db() first.")
  }
  
  tryCatch({
    if (fetch) {
      if (is.null(params)) {
        result <- DBI::dbGetQuery(conn, query)
      } else {
        result <- DBI::dbGetQuery(conn, query, params = params)
      }
      return(result)
    } else {
      if (is.null(params)) {
        DBI::dbExecute(conn, query)
      } else {
        DBI::dbExecute(conn, query, params = params)
      }
      return(NULL)
    }
  }, error = function(e) {
    stop(paste("Error executing query:", e$message))
  })
}

#' Fetch all data from a table
#'
#' This function retrieves all rows from a specified table.
#'
#' @param table_name The name of the table to fetch data from
#' @param schema The schema name (optional, defaults to public)
#'
#' @return A data frame containing all rows from the specified table
#'
#' @importFrom DBI dbQuoteIdentifier
#' @export
fetch_table <- function(table_name, schema = "public") {
  conn <- get_connection()
  if (is.null(conn)) {
    stop("No active database connection. Use connect_db() first.")
  }
  
  schema_quoted <- DBI::dbQuoteIdentifier(conn, schema)
  table_quoted <- DBI::dbQuoteIdentifier(conn, table_name)
  
  query <- paste("SELECT * FROM", schema_quoted, ".", table_quoted)
  
  tryCatch({
    result <- execute_query(query)
    return(result)
  }, error = function(e) {
    stop(paste("Error fetching table:", e$message))
  })
}

#' Fetch specific columns from a table
#'
#' This function retrieves specified columns from a table.
#'
#' @param table_name The name of the table to fetch data from
#' @param columns A character vector of column names to fetch
#' @param schema The schema name (optional, defaults to public)
#'
#' @return A data frame containing the specified columns from the table
#'
#' @importFrom DBI dbQuoteIdentifier
#' @export
fetch_columns <- function(table_name, columns, schema = "public") {
  conn <- get_connection()
  if (is.null(conn)) {
    stop("No active database connection. Use connect_db() first.")
  }
  
  schema_quoted <- DBI::dbQuoteIdentifier(conn, schema)
  table_quoted <- DBI::dbQuoteIdentifier(conn, table_name)
  columns_quoted <- sapply(columns, function(col) DBI::dbQuoteIdentifier(conn, col))
  columns_string <- paste(columns_quoted, collapse = ", ")
  
  query <- paste("SELECT", columns_string, "FROM", schema_quoted, ".", table_quoted)
  
  tryCatch({
    result <- execute_query(query)
    return(result)
  }, error = function(e) {
    stop(paste("Error fetching columns:", e$message))
  })
}
