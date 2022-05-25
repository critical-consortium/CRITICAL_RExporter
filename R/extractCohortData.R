
#break up the single SQL file into individual statements and output file names
parse_sql <- function(sqlFile) {
  sql <- ""
  output_file_tag <- "OUTPUT_FILE:"
  inrows <- unlist(strsplit(sqlFile, "\n"))
  statements <- list()
  outputs <- list()
  statementnum <- 0

  for (i in 1:length(inrows)) {
    sql = paste(sql, inrows[i], sep = "\n")
    if (regexpr("OUTPUT_FILE", inrows[i]) != -1) {
      output_file <- sub("--OUTPUT_FILE: ", "", inrows[i])
    }
    if (regexpr(";", inrows[i]) != -1) {
      statementnum <- statementnum + 1
      statements[[statementnum]] = sql
      outputs[[statementnum]] = output_file
      sql <- ""
    }
  }

  mapply(c, outputs, statements)

}

runExtraction  <- function(connectionDetails,
                           sqlFilePath,
                           cdmDatabaseSchema,
                           cohortDatabaseSchema,
                           outputFolder = paste0(getwd(), "/output/"),
                           useAndromeda = FALSE,
                           ...
                           )
{
  # workaround to avoid scientific notation
  # save current scipen value
  scipen_val <- getOption("scipen")
  # temporarily change scipen setting (restored at end of f())
  options(scipen=999)

  # create output dir if it doesn't already exist
  if (!file.exists(file.path(outputFolder)))
    dir.create(file.path(outputFolder), recursive = TRUE)

  if (!file.exists(paste0(outputFolder,"DATAFILES")))
    dir.create(paste0(outputFolder,"DATAFILES"), recursive = TRUE)

  # load source sql file
  src_sql <- SqlRender::readSql(sqlFilePath)

  # replace parameters with values
  src_sql <- SqlRender::render(sql = src_sql,
                               warnOnMissingParameters = FALSE,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               ...)


  # split script into chunks (to produce separate output files)
  allSQL <- parse_sql(src_sql)


  # establish database connection
  conn <- DatabaseConnector::connect(connectionDetails)

  #iterate through query list
  for (i in seq(from = 1, to = length(allSQL), by = 2)) {
    fileNm <- allSQL[i]

    # check for and remove return from file name
    fileNm <- gsub(pattern = "\r", x = fileNm, replacement = "")

    sql <- allSQL[i+1]

    output_path <- outputFolder


    if(useAndromeda){

      executeChunkAndromeda(conn = conn,
                             sql = sql,
                             fileName = fileNm,
                             outputFolder = output_path)
    }else{

      num_result_rows <- executeChunk(conn = conn,
                                      sql = sql,
                                      fileName = fileNm,
                                      outputFolder = output_path)


    }



  }



  # Disconnect from database
  DatabaseConnector::disconnect(conn)

  # restore original scipen value
  options(scipen=scipen_val)

}


executeChunk <- function(conn,
                         sql,
                         fileName,
                         outputFolder){



  result <- DatabaseConnector::querySql(conn, sql)

  

    write.table(result, file = paste0(outputFolder, fileName ), sep = "|", row.names = FALSE, na="")
    return(nrow(result))
}



executeChunkAndromeda <- function(conn,
                                 sql,
                                 fileName,
                                 outputFolder){



  andr <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(connection = conn
                                         ,sql = sql
                                         ,andromeda = andr
                                         ,andromedaTableName = "tmp")

  write.table(andr$tmp, file = paste0(outputFolder, fileName ), sep = "|", row.names = FALSE, na="")

  Andromeda::close(andr)


}


