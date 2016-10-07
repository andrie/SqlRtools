sp_template <- "CREATE PROCEDURE [SqlSProc1]
AS
BEGIN
EXEC sp_execute_external_script @language = N'R'
, @script = N'_RCODE_'
, @input_data_1 = N'_INPUT_QUERY_'
--- Edit this line to handle the output data frame.
WITH RESULT SETS (_CLASSES_);
END;"


#' Generate stored procedure from SQL code and R template.
#' 
#' Given two files xx.R and xx.sql, generate a stored procedure and optionally save as xx.sql
#' 
#' @param sp_filename_stub File name stub for R code and SQL code, both stored as files with the same name but varying extensions.
#' @param dbConnection Database connection string, passed to \code{\link[RODBC]odbcDriverConnect}
#' @param write If TRUE, writes results to xx.sp.sql
#' 
#' @importFrom RODBC odbcDriverConnect sqlQuery
#' 
#' @export
generate_stored_procedure <- function(sp_filename_stub, dbConnection, write = FALSE) {
  spi_query <- paste0(sp_filename_stub, ".Query.sql")
  spi_templ <- paste0(sp_filename_stub, ".Template.sql")
  spi_r <- paste0(sp_filename_stub, ".R")
  spi_output <- paste0(sp_filename_stub, ".sp.sql")
  
  sql_r_conv <- c(
    character = "varchar(max)", 
    integer   = "bigint", 
    logical   = "bit", 
    numeric   = "float", 
    Date      = "date", 
    PosixCT   = "datetime", 
    raw       = "smallint"
  )
  
  spi_read <- function(x) {
    iconv(
      paste(readLines(x, encoding = "UTF8", warn = FALSE), collapse = "\n"),
      from = "UTF8", to = "ASCII", sub = ""
    )
  }
  
  spi_outclasses <- function(query_file, query_r, dbConnection) {
    require(RODBC)
    qry <- spi_read(query_file)
    conn <- odbcDriverConnect(dbConnection)
    on.exit(close(conn))
    InputDataSet <- sqlQuery(conn, qry)
    #eval(parse(text = gsub("OutputDataSet <- ", "", spi_read(query_r))))
    eval(parse(text = spi_read(query_r)))
    
    classes <- vapply(OutputDataSet, class, FUN.VALUE = character(1))
    sql_classes <- setNames(sql_r_conv[classes], names(OutputDataSet))
    sprintf("(%s)", paste(names(sql_classes), sql_classes, collapse = ", "))
  }
  outclasses <- spi_outclasses(spi_query, spi_r, dbConnection)
  
  RCODE <- strsplit(spi_read(spi_r), "\n")[[1]]
  RCODE <- gsub("^ *#.*$", "", RCODE)
  RCODE <- paste(RCODE, collapse = "\n")

  res <- sub("_RCODE", RCODE,
             sub("_INPUT_QUERY_", spi_read(spi_query),
                 sub("_CLASSES_", outclasses, spi_read(spi_templ))
             ))
  
  con <- file(spi_output, open = "wt")
  on.exit(close(con))
  if (write) writeLines(res, con = con) else res
  
}

