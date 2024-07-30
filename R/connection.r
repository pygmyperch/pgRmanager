#' @title Establish a connection to the PostgreSQL database
#' @description This function creates a connection to a PostgreSQL database using the provided parameters.
#' @param dbname The name of the database
#' @param host The database server host
#' @param port The port number
#' @param user The username for the database
#' @param password The password for the database
#' @return A database connection object
#' @importFrom RPostgreSQL PostgreSQL
#' @importFrom DBI dbConnect
#' @export
connect_db <- function(dbname, host = "localhost", port = 5432, user = NULL, password = NULL) {
  if (!requireNamespace("RPostgreSQL", quietly = TRUE)) {
    stop("RPostgreSQL package is required. Please install it using install.packages('RPostgreSQL')")
  }
  
  drv <- RPostgreSQL::PostgreSQL()
  
  tryCatch({
    con <- DBI::dbConnect(drv, 
                          dbname = dbname,
                          host = host,
                          port = port,
                          user = user,
                          password = password)
    
    # Store the connection in the package environment
    pkg_env$current_connection <- con
    
    message("Successfully connected to the database.")
    return(con)
  }, error = function(e) {
    stop(paste("Failed to connect to the database:", e$message))
  })
}

#' @title Close the database connection
#' @description This function closes the current database connection.
#' @return NULL
#' @importFrom DBI dbDisconnect
#' @export
disconnect_db <- function() {
  if (!is.null(pkg_env$current_connection)) {
    DBI::dbDisconnect(pkg_env$current_connection)
    pkg_env$current_connection <- NULL
    message("Database connection closed.")
  } else {
    message("No active database connection to close.")
  }
}

#' @title Retrieve the current database connection
#' @description This function returns the current database connection object.
#' @return A database connection object or NULL if no connection is active
#' @export
get_connection <- function() {
  if (is.null(pkg_env$current_connection)) {
    message("No active database connection. Use connect_db() to establish a connection.")
    return(NULL)
  }
  return(pkg_env$current_connection)
}

# Create a package environment to store the current connection
pkg_env <- new.env(parent = emptyenv())
pkg_env$current_connection <- NULL
