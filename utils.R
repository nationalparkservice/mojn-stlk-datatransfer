# Function for inserting data into the database
uploadData <- function(df, table.name, conn, guid = FALSE) {
  # Build SQL statements
  sql.insert <- ""
  
  ## Modify SQL statement based on whether the GUID globalid column from AGOL is being stored in the database
  if (guid) {
    guid.col <- "INSERTED.GUID"
  } else {
    guid.col <- "NULL AS GUID"
  }
  
  cols <- paste(names(df), collapse = ", ")
  placeholders <- rep("?", length(names(df)))
  placeholders <- paste(placeholders, collapse = ", ")
  sql.insert <- paste0("INSERT INTO ", table.name, "(", cols, ") ",
                      "OUTPUT INSERTED.ID, INSERTED.GUID INTO InsertOutput ",
                      "VALUES (",
                      placeholders,
                      ") ")
  
  sql.inserted <- "SELECT * FROM InsertOutput"
  
  # Perform insert
  keys <- tibble()
  keys <- poolWithTransaction(pool = conn, func = function(conn) {
    dbCreateTable(conn, "InsertOutput", tibble(ID = integer(), GUID = character()))
    
    qry <- dbSendQuery(conn, sql.insert)
    dbBind(qry, as.list(df))
    dbFetch(qry)
    dbClearResult(qry)
    
    res <- dbSendQuery(conn, sql.inserted)
    inserted <- dbFetch(res) %>%
      as_tibble()
    dbClearResult(res)
    
    dbRemoveTable(conn, "InsertOutput")
    
    return(inserted)
  })
  
  if (!guid) {
    keys <- select(keys, ID)
  }
  
  return(keys)
}