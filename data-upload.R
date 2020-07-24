# Upload data to MOJN_STLK database

#---------Settings----------#
# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_AnnualLakeVisit_20191022.gdb"
gdb.path <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\MOJN_STLK_AnnualLakeVisit_20200130.gdb"
# photo.dest <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\ImageData\\Lakes"
# originals.dest <- "M:\\MONITORING\\_FieldPhotoOriginals_DoNotModify\\AGOL_STLK"
photo.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest"
originals.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\Originals"
db.params.path <- "C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv"
#---------------------------#

db <- list()

## Get Site table from database
params <- readr::read_csv(db.params.path) %>%  # TODO: Change to real database connection after testing is done
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
photo.table <- paste(gdb.path, "Photos__ATTACH", sep = "\\")
visit.data <- paste(gdb.path, "MOJN_STLK_AnnualLakeVisit", sep = "\\")
photo.data <- paste(gdb.path, "Photos", sep = "\\")
py.ver <- py_config()
if (!(py.ver$version < 3)) {
  use_python("C:\\Python27\\ArcGISx6410.5", required = TRUE)
}
source_python("download-photos.py")

## Create Python dictionaries from photo and site lookups
photo.type.dict <- py_dict(photo.types$ID, photo.types$Code)
lake.code.dict <- py_dict(sites$ID, sites$CodeFull)

## Download photos from annual lake visits
annual_photos <- download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest, photoCodeDict = photo.type.dict, lakeCodeDict = lake.code.dict)
annual_photos <- as_tibble(annual_photos)
annual_photos$VisitGUID <- str_remove_all(annual_photos$VisitGUID, "\\{|\\}")
annual_photos$GlobalID <- str_remove_all(annual_photos$GlobalID, "\\{|\\}")

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
         DataProcessingLevelID = 1  # Raw
         ) %>%
  left_join(select(sites, CodeFull, ID, ProtectedStatusID), by = c("SiteID" = "ID")) %>%
  select(-StartDateTime, -CodeFull) 
visit.keys <- uploadData(db$Visit, "data.Visit", conn, keep.guid = TRUE)  # Insert into Visit table in database

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
db$PhotoActivity <- visit %>%  # All the photo activity data actually just comes from visit!
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         CameraID,
         CameraCardID) %>%
  unique()
photoact.keys <- uploadData(db$PhotoActivity, "data.PhotoActivity", conn, keep.guid = FALSE)
names(photoact.keys) <- c("PhotoActivityID", "VisitGlobalID")

## Photo table
db$Photo <- annual_photos %>%
  mutate(VisitGUID = tolower(VisitGUID),
         GlobalID = tolower(GlobalID)) %>%
  inner_join(photoact.keys, by = c("VisitGUID" = "VisitGlobalID")) %>%
  inner_join(photos, by = c('GlobalID' = 'globalid')) %>%
  select(PhotoActivityID,
         PhotoDescriptionCodeID = PhotoTypeID,
         IsLibraryPhotoID = IsLibrary,
         OriginalFilePath,
         RenamedFilePath,
         GPSUnit = GPS_Photo,
         X_coord = x,
         Y_coord = y,
         WKID = wkid,
         Notes = PhotoNotes)
photo.keys <- uploadData(db$Photo, "data.Photo", conn, keep.guid = FALSE)

## VisitPersonnel table
db$VisitPersonnel <- crew %>%
  inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         PersonnelID = Initials) %>%
  mutate(PersonnelRoleID = 5)  # Field crew
personnel.keys <- uploadData(db$VisitPersonnel, "data.VisitPersonnel", conn, keep.guid = FALSE, cols.key = list(VisitID = integer(), PersonnelID = integer(), PersonnelRoleID = integer()))

## WaterQualityActivity table
db$WaterQualityActivity <- visit %>%
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         VisitID = ID,
         WaterQualityDataCollectedID = WQTaken,
         StartTime = StartDateTime,
         pHInstrumentID,
         DOInstrumentID,
         SpCondInstrumentID,
         TemperatureInstrumentID,
         Notes = WQNote) %>%
  mutate(DataProcessingLevelID = 1,  # Raw
         StartTime = strftime(StartTime, format = "%T"))
wqactivity.keys <- uploadData(db$WaterQualityActivity, "data.WaterQualityActivity", conn, keep.guid = FALSE)

## WaterQualityDepthProfile table
db$WQDepthProfile <- wq %>%
  inner_join(wqactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityActivityID = ID,
         IsDepthProfile,
         MeasurementDepth_ft) %>%
  unique()
wqdepthprofile.keys <- uploadData(db$WQDepthProfile, "data.WaterQualityDepthProfile", conn, keep.guid = FALSE)

if (any(is.na(wq$MeasurementDepth_ft)) | any(wq$IsDepthProfile != "Y")) {
  warning("The data contain measurements that are not part of a depth profile.")
}

## WaterQualityDepthProfileDO table
do1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_1,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_1,
         DataQualityFlagID = DO_Flag_1,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 1)

do2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_2,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_2,
         DataQualityFlagID = DO_Flag_2,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 2)

do3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_3,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_3,
         DataQualityFlagID = DO_Flag_3,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 3)

db$WQDissolvedOxygen <- rbind(do1, do2, do3) %>% arrange(WaterQualityDepthProfileID)
do.keys <- uploadData(db$WQDissolvedOxygen, "data.WaterQualityDepthProfileDO", conn)

## WaterQualityDepthProfilepH table
ph1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         pH = pH_1,
         DataQualityFlagID = pH_Flag_1,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 1)

ph2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         pH = pH_2,
         DataQualityFlagID = pH_Flag_2,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 2)

ph3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         pH = pH_3,
         DataQualityFlagID = pH_Flag_3,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 3)

db$WQpH <- rbind(ph1, ph2, ph3) %>% arrange(WaterQualityDepthProfileID)
ph.keys <- uploadData(db$WQpH, "data.WaterQualityDepthProfilepH", conn)

## WaterQualityDepthProfileSpCond table
spcond1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_1,
         DataQualityFlagID = SpCond_Flag_1,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 1)

spcond2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_2,
         DataQualityFlagID = SpCond_Flag_2,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 2)

spcond3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_3,
         DataQualityFlagID = SpCond_Flag_3,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 3)

db$WQSpCond <- rbind(spcond1, spcond2, spcond3) %>% arrange(WaterQualityDepthProfileID)
spcond.keys <- uploadData(db$WQSpCond, "data.WaterQualityDepthProfileSpCond", conn)

## WaterQualityDepthProfileTemp table
temp1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_1,
         DataQualityFlagID = Temp_Flag_1,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 1)

temp2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_2,
         DataQualityFlagID = Temp_Flag_2,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 2)

temp3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_3,
         DataQualityFlagID = Temp_Flag_3,
         DataQualityFlagNote = FlagNote) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 3)

db$WQTemp <- rbind(temp1, temp2, temp3) %>% arrange(WaterQualityDepthProfileID)
temp.keys <- uploadData(db$WQTemp, "data.WaterQualityDepthProfileTemperature", conn)

## ClarityActivity table
db$ClarityActivity <- visit %>%
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         VisitID = ID,
         PersonnelID = SecchiObserverID,
         DiskOnBottomID = SecchiVisBottom,
         SurfaceCalmID = IsSurfaceCalm,
         DepthToBottom_ft = LakeDepth_ft,
         ObservationTime = StartDateTime) %>%
  mutate(DataProcessingLevelID = 1,
         ObservationTime = strftime(ObservationTime, format = "%T"))
clarityactivity.keys <- uploadData(db$ClarityActivity, "data.ClarityActivity", conn)

## ClaritySecchiDepth table
desc <- secchi %>%
  inner_join(clarityactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(ClarityActivityID = ID,
         Depth_ft = DescendingDepth_ft) %>%
  mutate(DepthTypeID = 2) # Descending

asc <- secchi %>%
  inner_join(clarityactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(ClarityActivityID = ID,
         Depth_ft = AscendingDepth_ft) %>%
  mutate(DepthTypeID = 3) # Ascending)

db$SecchiDepth <- rbind(desc, asc) %>% arrange(ClarityActivityID, DepthTypeID)

secchi.keys <- uploadData(db$SecchiDepth, "data.ClaritySecchiDepth", conn)

## WaterChemistryActivity table
db$WaterChemistryActivity <- visit %>%
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(VisitID = ID,
         SampleCollectionMethodID = SampleMethodID,
         NumberOfBottlesFiltered = BottleCountFiltered,
         NumberOfBottlesUnfiltered = BottleCountUnfiltered,
         Notes = SampleNote) %>%
  mutate(DataProcessingLevelID = 1,
         LaboratoryID = 1)  # CCAL

chem.keys <- uploadData(db$WaterChemistryActivity, "data.WaterChemistryActivity", conn)


pool::poolClose(conn)
