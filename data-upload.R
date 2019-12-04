# Upload data to MOJN_STLK database

db <- list()

## Get Site table from database
params <- readr::read_csv("C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv") %>%  # TODO: Change to real database connection after testing is done
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)
sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()

## Visit table
db$Visit <- visit %>%
  select(LakeCode, StartDateTime, Notes = OverallNotes, GUID = globalid) %>%
  mutate(VisitGroupID = 27,  # TODO: Add to app
         VisitDate = format.Date(StartDateTime, "%Y-%m-%d"),
         StartTime = format.Date(StartDateTime, "%H:%M:%S"),
         VisitTypeID = 1,  # TODO: Add to app
         MonitoringStatusID = 1,  # TODO: Add to app
         ProtocolID = 2,  # TODO: Add to app
         IsLakeDry = 0, # TODO: Add to app
         DataProcessingLevelID = 1
         ) %>%
  left_join(select(sites, CodeFull, ID, ProtectedStatusID), by = c("LakeCode" = "CodeFull")) %>%
  select(-LakeCode, -StartDateTime) %>%
  rename(SiteID = ID)

## Insert into Visit table in database
visit.keys <- uploadData(db$Visit, "data.Visit", conn, guid = TRUE)

pool::poolClose(conn)
