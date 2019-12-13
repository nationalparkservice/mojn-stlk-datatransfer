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

visit.keys <- uploadData(db$Visit, "data.Visit", conn, guid = TRUE)  # Insert into Visit table in database
visit.keys <- mutate(visit.keys, GUID = tolower(GUID))

## LoggerDeploy table
db$LoggerDeploy <- sensor.deploy %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GUID")) %>%
  select(VisitID = ID,
         GUID = globalid,
         LoggerMediumID = LoggerType,
         LoggerID,
         LoggerSerial = OtherLoggerID,
         GPSName = GPS_PTnew,
         X = x,
         Y = y,
         wkid
  )
loggerdeploy.keys <- uploadData(db$LoggerDeploy, "data.LoggerDeploy", conn, guid = FALSE)

## LoggerDownload table
db$LoggerDownload <- sensor.dl %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GUID")) %>%
  select(VisitID = ID,
         GUID = globalid,
         LoggerMediumID = LoggerType_old,
         LoggerID = LoggerID_old,
         LoggerSerial = OtherLoggerID_old,
         Downloaded = DownloadState,
         OriginalFileName = FileName,
         GPSName = GPS_PTold,
         X = x,
         Y = y,
         wkid
  )
loggerdl.keys <- uploadData(db$LoggerDownload, "data.LoggerDownload", conn, guid = FALSE)

## PhotoActivity table

## Photo table


## VisitPersonnel table


## WaterQualityActivity table


## WaterQualityDepthProfile table


## WaterQualityDepthProfileDO table


## WaterQualityDepthProfilepH table


## WaterQualityDepthProfileSpCond table


## WaterQualityDepthProfileTemp table


## ClarityActivity table


## ClaritySecchiDepth table


## WaterChemistryActivity table



pool::poolClose(conn)
