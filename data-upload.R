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
  select(SiteID = LakeCode,
         StartDateTime,
         Notes = OverallNotes,
         GlobalID = globalid,
         VisitGroupID,
         VisitTypeID,
         MonitoringStatusID,
         GPSUnitID,
         ProtocolID = ProtocolPackageID,
         IsLakeDry) %>%
  mutate(VisitDate = format.Date(StartDateTime, "%Y-%m-%d"),
         StartTime = format.Date(StartDateTime, "%H:%M:%S"),
         DataProcessingLevelID = 1
         ) %>%
  left_join(select(sites, CodeFull, ID, ProtectedStatusID), by = c("SiteID" = "ID")) %>%
  select(-StartDateTime, -CodeFull) 

visit.keys <- uploadData(db$Visit, "data.Visit", conn, keep.guid = TRUE)  # Insert into Visit table in database
visit.keys <- mutate(visit.keys, GlobalID = tolower(GlobalID))

## LoggerDeploy table
db$LoggerDeploy <- sensor.deploy %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         LoggerMediumID,
         LoggerID,
         LoggerSerial = OtherLoggerSN,
         X_coord = x,
         Y_coord = y,
         WKID = wkid
  )
loggerdeploy.keys <- uploadData(db$LoggerDeploy, "data.LoggerDeployment", conn, keep.guid = TRUE)

## LoggerDownload table
db$LoggerDownload <- sensor.dl %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         LoggerMediumID = LoggerMediumID_old,
         LoggerID = LoggerID_old,
         LoggerSerial = OtherLoggerSN_old,
         Downloaded_YN = DownloadState,
         OriginalFileName = FileName,
         X_coord = x,
         Y_coord = y,
         WKID = wkid
  )
loggerdl.keys <- uploadData(db$LoggerDownload, "data.LoggerDownload", conn, keep.guid = TRUE)

## PhotoActivity table
db$PhotoActivity <- photos %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GUID")) %>%
  select(VisitID = ID) %>%
  mutate(CameraID = 1,  # TODO: Add to app
         CameraCardID = 1,  # TODO: Add to app
         DataProcessingLevelID = 1) %>%
  unique()
photos.keys <- uploadData(db$PhotoActivity, "data.PhotoActivity", conn, keep.guid = FALSE)

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
