# Upload data to MOJN_STLK database

db <- list()

## Get Site table from database
params <- readr::read_csv("C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv") %>%  # TODO: Change to real database connection after testing is done
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)

sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()

# Download photos using Python script

# Get lookups of photo codes for photo naming
photo.types <- dplyr::tbl(conn, dbplyr::in_schema("ref", "PhotoDescriptionCode")) %>%
  dplyr::collect() %>%
  select(ID, Code)

# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_AnnualLakeVisit_20191022.gdb"
gdb.path <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\MOJN_STLK_AnnualLakeVisit_20200130.gdb"
photo.table <- paste(gdb.path, "Photos__ATTACH", sep = "\\")
visit.data <- paste(gdb.path, "MOJN_STLK_AnnualLakeVisit", sep = "\\")
photo.data <- paste(gdb.path, "Photos", sep = "\\")
# photo.dest <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\ImageData\\Lakes"
# originals.dest <- "M:\\MONITORING\\_FieldPhotoOriginals_DoNotModify\\AGOL_STLK"
photo.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest"
originals.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\Originals"
use_python("C:\\Python27\\ArcGISx6410.5", required = TRUE)
source_python("download-photos.py")

## Create Python dictionaries from photo and site lookups
photo.type.dict <- py_dict(photo.types$ID, photo.types$Code)
lake.code.dict <- py_dict(sites$ID, sites$CodeFull)

## Download photos from annual lake visits
annual_photos <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest, photoCodeDict = photo.type.dict, lakeCodeDict = lake.code.dict)
annual_photos <- as_tibble(annual_photos)
annual_photos$VisitGUID <- str_remove_all(annual_photos$VisitGUID, "\\{|\\}")
annual_photos$GlobalID <- str_remove_all(annual_photos$GlobalID, "\\{|\\}")

# TODO: Consider taking all benchmark photos in annual lake visit survey
## Download benchmark photos
# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_LakeLevels_20191022.gdb"
gdb.path <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\STLK_LakeLevels_20191022.gdb"
photo.table <- paste(gdb.path, "BenchPhoto__ATTACH", sep = "\\")
visit.data <- paste(gdb.path, "Form_2", sep = "\\")
photo.data <- paste(gdb.path, "BenchPhoto", sep = "\\")

bench_photos <- download_bench_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest)
bench_photos <- as_tibble(bench_photos)
bench_photos$VisitGUID <- str_remove_all(bench_photos$VisitGUID, "\\{|\\}")

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
