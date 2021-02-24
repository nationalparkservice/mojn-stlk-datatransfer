# Upload data to MOJN_STLK database

#--------pull tables-----------#
# connect to SQl DB
params <- readr::read_csv(db.params.path) %>%  # TODO: Change to real database connection after testing is done
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)

## Get needed tables from database for joining and comparing records
sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()

# get Visit table ID's, Global ID's, and last edited date 
visitIDs <- dplyr::tbl(conn, dbplyr::in_schema("data", "Visit")) %>% 
  dplyr::select(ID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))

# get LoggerDeployment table ID's, Global ID's, and last edited date   
loggerDeployIDs<- dplyr::tbl(conn, dbplyr::in_schema("data", "LoggerDeployment")) %>% 
  dplyr::select(ID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))

# get LoggerDownload table ID's, Global ID's, and last edited date   
loggerDownloadIDs<- dplyr::tbl(conn, dbplyr::in_schema("data", "LoggerDownload")) %>% 
  dplyr::select(ID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))

# get SecchiMeasurements table ID's, Global ID's, and last edited date   
secchiIDs<- dplyr::tbl(conn, dbplyr::in_schema("data", "ClaritySecchiDepth")) %>% 
  dplyr::select(ID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))


############# For 2020 data only #####################################
# please run in 2021 once only. this will populate the VisitPersonell table with the survey123 edited date and global ID from FieldCrew where there are already records in the DB. Thats so the merge code will run properly later. after the initial update, it won't ever need doing again!
# VP_SQLupdate <- crew %>%
#   inner_join(visit.keys, by = c("parentglobalid" = "GlobalID")) %>%
#   select(VisitID = ID,
#          GlobalID = globalid,
#          PersonnelID = Initials,
#          Survey123_LastEditedDate) %>% 
#   mutate(PersonnelRoleID = 5)
# 
# sql.update = paste0("UPDATE data.VisitPersonnel",
#                    " SET data.VisitPersonnel.GlobalID = t.GlobalID,",
#                    " data.VisitPersonnel.Survey123_LastEditedDate = t.Survey123_LastEditedDate",
#                    " FROM data.VisitPersonnel as p",
#                    " INNER JOIN dbo.Temp as t",
#                    " ON (p.VisitID = t.VisitID AND p.PersonnelID = t.PersonnelID AND p.PersonnelRoleID = t.PersonnelRoleID)")
# 
# #insert temp target table 
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", VP_SQLupdate)
#   dbAppendTable(conn, "Temp", VP_SQLupdate)
# 
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
#   })
########## end update visit personell ######################################

## get Visit Personal table ID's, Global ID's, and last edited date. Compare with s123 crew table
visitPersonelIDs <- dplyr::tbl(conn, dbplyr::in_schema("data", "VisitPersonnel")) %>% 
  dplyr::collect() %>% 
  dplyr::select(VisitID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::rename(SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate)

## get WQ activity table ID's, Global ID's, and last edited date 
WQactivityIDs <- dplyr::tbl(conn, dbplyr::in_schema("data", "WaterQualityActivity")) %>% 
  dplyr::select(ID,GlobalID) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))

############# For 2020 data only #####################################
# This will populate the Water Quality Depth Profile table with the survey123 edited date and global ID from WQ reading where there are already records in the DB. Thats so the merge code will run properly later. after the iunitial update, it wont ever need doing again!
# WQR_SQLupdate <- wq %>%
#   inner_join(wqactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
#   select(WaterQualityActivityID = ID,
#          GlobalID = globalid,
#          Survey123_LastEditedDate,
#          IsDepthProfile,
#          MeasurementDepth_ft) %>%
#   unique()
# 
# sql.update = paste0("UPDATE data.WaterQualityDepthProfile",
#                     " SET data.WaterQualityDepthProfile.GlobalID = t.GlobalID,",
#                     " data.WaterQualityDepthProfile.Survey123_LastEditedDate = t.Survey123_LastEditedDate",
#                     " FROM data.WaterQualityDepthProfile as p",
#                     " INNER JOIN dbo.Temp as t",
#                     " ON (p.WaterQualityActivityID = t.WaterQualityActivityID AND p.MeasurementDepth_ft = t.MeasurementDepth_ft)")
# 
# #insert temp target table 
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", WQR_SQLupdate)
#   dbAppendTable(conn, "Temp", WQR_SQLupdate)
#   
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
# })
########## end update WQ depth profile ######################################

#get WQ reading table ID's, Global ID's
WQreadingIDs <-dplyr::tbl(conn, dbplyr::in_schema("data", "WaterQualityDepthProfile")) %>% 
  dplyr::collect() %>% 
  dplyr::select(ID,GlobalID,Survey123_LastEditedDate) %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID, SQL.Survey123_LastEditedDate = Survey123_LastEditedDate)


# get Secchi activity table ID's, Global ID's
SecchiActivityIDs <- dplyr::tbl(conn, dbplyr::in_schema("data", "ClarityActivity")) %>% 
  dplyr::select(ID,GlobalID) %>% 
  dplyr::collect() %>% 
  dplyr::rename(SQL.ID = ID, SQL.GlobalID = GlobalID) %>% 
  dplyr::mutate(SQL.GlobalID = tolower(SQL.GlobalID))

#--------------------------------#

#--------- Python Script for photos ----------------#
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
#--------------------------------#


#---------munge tables and upload--------#
db <- list()

## Visit table
# build the base visit table of records that are new or  are already in the SQL DB but have been updated in S123
# (converts dates to chars for compare only) - added row to look for existing records in DB and S123 that were imported from S123 before this script and don't have a survey123 date yet. This or statement can be removed after this initial import
baseV <- visit %>%
  left_join(visitIDs, by = c("globalid" = "SQL.GlobalID" )) %>%
  filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate)
         | is.na(SQL.ID)
         | is.na(SQL.Survey123_LastEditedDate)& !is.na(SQL.ID))


# build db$visit
db$Visit <- baseV %>%
  select(SiteID = LakeCode,
         StartDateTime,
         Notes = OverallNotes,
         GlobalID = globalid,
         VisitGroupID,
         VisitTypeID,
         MonitoringStatusID,
         WindSpeedID = WindSpeed,
         CloudCoverageID = Cloud,
         PrecipitationID = Precip,
         Temperature_F_ID = Temp_deg_F,
         ProtocolID = ProtocolPackageID,
         IsLakeDry,
         Survey123_LastEditedDate) %>%
  mutate(VisitDate = format.Date(StartDateTime, "%Y-%m-%d"),
         StartTime = format.Date(StartDateTime, "%H:%M:%S"),
         DataProcessingLevelID = 1  # Raw
         ) %>%
  left_join(select(sites, CodeFull, ID, ProtectedStatusID), by = c("SiteID" = "ID")) %>%
  select(-StartDateTime, -CodeFull) 

visit.keys <- uploadData(db$Visit, "data.Visit", conn, keep.guid = TRUE)  # Insert into Visit table in database

## need to make a full visit keys list of all guids and IDs not just the new ones, so need to merge visit keys with visit IDs
fullVisit.keys <- visit.keys %>% 
  select(ID, GlobalID) %>% 
  union(rename(subset(visitIDs, select = c(SQL.ID, SQL.GlobalID)),
               ID = SQL.ID, GlobalID = SQL.GlobalID ))


## LoggerDeploy table
# build the base logger deploy table of records that are new or  are already in the SQL DB but have been updated in S123
# (converts dates to chars for compare only)
baseLDeploy <- sensor.deploy %>% 
  left_join(loggerDeployIDs, by = c("globalid" = "SQL.GlobalID" )) %>% 
  filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate) 
         | is.na(SQL.ID)
         | is.na(SQL.Survey123_LastEditedDate)& !is.na(SQL.ID))


db$LoggerDeploy <- baseLDeploy %>%
  inner_join(fullVisit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         LoggerMediumID,
         LoggerID,
         LoggerSerial = OtherLoggerSN,
         X_coord = x,
         Y_coord = y,
         WKID = wkid,
         Survey123_LastEditedDate)

loggerdeploy.keys <- uploadData(db$LoggerDeploy, "data.LoggerDeployment", conn, keep.guid = TRUE)

## LoggerDownload table
# build the base logger download table of records that are new or  are already in the SQL DB but have been updated in S123
# (converts dates to chars for compare only)
baseLDownload <- sensor.dl %>% 
  left_join(loggerDeployIDs, by = c("globalid" = "SQL.GlobalID" )) %>% 
  filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate) 
         | is.na(SQL.ID)
         | is.na(SQL.Survey123_LastEditedDate)& !is.na(SQL.ID))

db$LoggerDownload <- baseLDownload %>%
  inner_join(fullVisit.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         LoggerMediumID = LoggerMediumID_old,
         LoggerID = LoggerID_old,
         LoggerSerial = OtherLoggerSN_old,
         Downloaded_YN = DownloadState,
         OriginalFileName = FileName,
         X_coord = x,
         Y_coord = y,
         WKID = wkid,
         Survey123_LastEditedDate)

loggerdl.keys <- uploadData(db$LoggerDownload, "data.LoggerDownload", conn, keep.guid = TRUE)

## PhotoActivity table
db$PhotoActivity <- visit %>%  # All the photo activity data actually just comes from visit!
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         CameraID,
         CameraCardID) %>%
  unique()

photoact.keys <- uploadData(db$PhotoActivity, "data.PhotoActivity", conn, keep.guid = TRUE)
names(photoact.keys) <- c("PhotoActivityID", "VisitGlobalID", "action")

## Photo table- because it uses images..
# I left as is theres only a notes field that could be updated later? would anyone actually update that? and it gets complicated with the photo python part
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
##looks for duplicates!!!!
dups<- crew %>% 
  count(parentglobalid,Initials) %>% 
  filter(n>1)

if (nrow(dups)>0){
  print("STOP! There are some duplicate crewnames - check in Survey123")
  dups
}else{
  ## build the visit personel table of records that are new or have been updated (convert dates to chars for compare only)
  baseVP <- crew %>% 
    left_join(visitPersonelIDs, by = c("globalid" = "SQL.GlobalID" )) %>% 
    filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate) | is.na(VisitID))
  
  db$VisitPersonnel <- baseVP %>%
    inner_join(fullVisit.keys, by = c("parentglobalid" = "GlobalID")) %>%
    select(VisitID = ID,
           GlobalID = globalid,
           PersonnelID = Initials,
           Survey123_LastEditedDate) %>%
    mutate(PersonnelRoleID = 5)  # Field crew
  
  personnel.keys <- uploadData(db$VisitPersonnel, "data.VisitPersonnel", conn, keep.guid = TRUE, cols.key = list(VisitID = integer(), PersonnelID = integer(), PersonnelRoleID = integer()))
  
}


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
wqactivity.keys <- uploadData(db$WaterQualityActivity, "data.WaterQualityActivity", conn, keep.guid = TRUE)

## WaterQualityDepthProfile table - related table so have to compare edited dates directly
# build the WQreading table of records that are new or have been updated (convert dates to chars for compare only)
baseWQ <- wq %>%
  left_join(WQreadingIDs, by = c("globalid" = "SQL.GlobalID" )) %>%
  filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate) | is.na(SQL.ID))

## need to make a full WQ activity keys list of all guids and IDs not just the new ones so need to merge disturbanceActivity keys with DistActivityIDs
fullWQactivity.keys <- wqactivity.keys %>%
  select(ID, GlobalID) %>%
  union(rename(subset(WQactivityIDs, select = c(SQL.ID, SQL.GlobalID)),
               ID = SQL.ID, GlobalID = SQL.GlobalID ))

db$WQDepthProfile <- baseWQ %>%
  inner_join(fullWQactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityActivityID = ID,
         IsDepthProfile,
         MeasurementDepth_ft,
         Survey123_LastEditedDate,
         pHDataQualityFlagID = pH_Flag,
         DissolvedOxygenDataQualityFlagID = DO_Flag,
         SpecificConductanceDataQualityFlagID = SpCond_Flag,
         WaterTemperatureDataQualityFlagID = Temp_Flag) %>%
  unique()
wqdepthprofile.keys <- uploadData(db$WQDepthProfile, "data.WaterQualityDepthProfile", conn, keep.guid = TRUE)

if (any(is.na(wq$MeasurementDepth_ft)) | any(wq$IsDepthProfile != "Y")) {
  warning("The data contain measurements that are not part of a depth profile.")
}


## WaterQualityDepthProfileDO table
do1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_1,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_1) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 1, GlobalID = paste0(GlobalID,"_1"))

do2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID =globalid,
         WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_2,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_2) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 2, GlobalID = paste0(GlobalID,"_2"))

do3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID =globalid,
         WaterQualityDepthProfileID = ID,
         DissolvedOxygen_percent = DO_percent_3,
         DissolvedOxygen_mg_per_L = DO_mg_per_L_3) %>%
  filter(!is.na(DissolvedOxygen_percent) | !is.na(DissolvedOxygen_mg_per_L)) %>%
  mutate(MeasurementNum = 3,GlobalID = paste0(GlobalID,"_3"))

db$WQDissolvedOxygen <- rbind(do1, do2, do3) %>% arrange(WaterQualityDepthProfileID)

############# For 2020 data only #####################################
# #this will update the records currently in thr SQL database. only needs running once
# sql.update = paste0("UPDATE data.WaterQualityDepthProfileDO",
#                     " SET data.WaterQualityDepthProfileDO.GlobalID = t.GlobalID",
#                     " FROM data.WaterQualityDepthProfileDO as p",
#                     " INNER JOIN dbo.Temp as t",
#                     " ON (p.WaterQualityDepthProfileID = t.WaterQualityDepthProfileID AND p.MeasurementNum = t.MeasurementNum)")
# 
# #insert temp target table 
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", db$WQDissolvedOxygen)
#   dbAppendTable(conn, "Temp", db$WQDissolvedOxygen)
#   
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
# })
############## end update WQ DO table ########################
do.keys <- uploadData(db$WQDissolvedOxygen, "data.WaterQualityDepthProfileDO", conn,keep.guid = TRUE)


## WaterQualityDepthProfilepH table
ph1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         pH = pH_1) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 1, GlobalID = paste0(GlobalID,"_1"))

ph2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         pH = pH_2) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 2,GlobalID = paste0(GlobalID,"_2"))

ph3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         pH = pH_3) %>%
  filter(!is.na(pH)) %>%
  mutate(MeasurementNum = 3,GlobalID = paste0(GlobalID,"_3"))

db$WQpH <- rbind(ph1, ph2, ph3) %>% arrange(WaterQualityDepthProfileID)

############################  For 2020 data only ############
# #this will update the records currently in thr SQL database. only needs running once
# sql.update = paste0("UPDATE data.WaterQualityDepthProfilepH",
#                     " SET data.WaterQualityDepthProfilepH.GlobalID = t.GlobalID",
#                     " FROM data.WaterQualityDepthProfilepH as p",
#                     " INNER JOIN dbo.Temp as t",
#                     " ON (p.WaterQualityDepthProfileID = t.WaterQualityDepthProfileID AND p.MeasurementNum = t.MeasurementNum)")
# 
# #insert temp target table
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", db$WQpH)
#   dbAppendTable(conn, "Temp", db$WQpH)
#   
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
# })
################ end update WQ pH table######################
ph.keys <- uploadData(db$WQpH, "data.WaterQualityDepthProfilepH", conn, keep.guid = TRUE)

## WaterQualityDepthProfileSpCond table
spcond1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_1) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 1, GlobalID = paste0(GlobalID,"_1"))

spcond2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_2) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 2, GlobalID = paste0(GlobalID,"_2"))

spcond3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         SpecificConductance_microS_per_cm = SpCond_microS_3) %>%
  filter(!is.na(SpecificConductance_microS_per_cm)) %>%
  mutate(MeasurementNum = 3, GlobalID = paste0(GlobalID,"_3"))

db$WQSpCond <- rbind(spcond1, spcond2, spcond3) %>% arrange(WaterQualityDepthProfileID)

############################ For 2020 data only ############
# #this will update the records currently in thr SQL database. only needs running once
# sql.update = paste0("UPDATE data.WaterQualityDepthProfileSpCond",
#                     " SET data.WaterQualityDepthProfileSpCond.GlobalID = t.GlobalID",
#                     " FROM data.WaterQualityDepthProfileSpCond as p",
#                     " INNER JOIN dbo.Temp as t",
#                     " ON (p.WaterQualityDepthProfileID = t.WaterQualityDepthProfileID AND p.MeasurementNum = t.MeasurementNum)")
# 
# #insert temp target table
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", db$WQSpCond)
#   dbAppendTable(conn, "Temp", db$WQSpCond)
#   
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
# })
############### end update WQ spcond table #######################
spcond.keys <- uploadData(db$WQSpCond, "data.WaterQualityDepthProfileSpCond", conn, keep.guid = TRUE)

## WaterQualityDepthProfileTemp table
temp1 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_1) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 1, GlobalID = paste0(GlobalID,"_1"))

temp2 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_2) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 2,GlobalID = paste0(GlobalID,"_2"))

temp3 <- wq %>%
  inner_join(wqdepthprofile.keys, by = c("globalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         WaterQualityDepthProfileID = ID,
         WaterTemperature_C = Temp_C_3) %>%
  filter(!is.na(WaterTemperature_C)) %>%
  mutate(MeasurementNum = 3, GlobalID = paste0(GlobalID,"_3"))

db$WQTemp <- rbind(temp1, temp2, temp3) %>% arrange(WaterQualityDepthProfileID)

############################ For 2020 data only ############
# #this will update the records currently in thr SQL database. only needs running once
# sql.update = paste0("UPDATE data.WaterQualityDepthProfileTemperature",
#                     " SET data.WaterQualityDepthProfileTemperature.GlobalID = t.GlobalID",
#                     " FROM data.WaterQualityDepthProfileTemperature as p",
#                     " INNER JOIN dbo.Temp as t",
#                     " ON (p.WaterQualityDepthProfileID = t.WaterQualityDepthProfileID AND p.MeasurementNum = t.MeasurementNum)")
# 
# #insert temp target table
# poolWithTransaction(pool = conn, func = function(conn) {
#   dbCreateTable(conn, "Temp", db$WQTemp)
#   dbAppendTable(conn, "Temp", db$WQTemp)
#   
#   qry <- dbSendQuery(conn, sql.update)
#   dbFetch(qry)
#   dbClearResult(qry)
#   dbRemoveTable(conn, "Temp")
# })
################ end update WQ temp table ######################
temp.keys <- uploadData(db$WQTemp, "data.WaterQualityDepthProfileTemperature", conn, keep.guid = TRUE)

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
clarityactivity.keys <- uploadData(db$ClarityActivity, "data.ClarityActivity", conn, keep.guid = TRUE)

## ClaritySecchiDepth table - related table so have to compare edited dates directly
# build the secci table of records that are new or have been updated (convert dates to chars for compare only)
baseSD <- secchi %>%
  left_join(secchiIDs, by = c("globalid" = "SQL.GlobalID" )) %>%
  filter(as.character(Survey123_LastEditedDate) != as.character(SQL.Survey123_LastEditedDate) | is.na(SQL.ID))

## need to make a full ClarityActivity  keys list of all guids and IDs not just the new ones so need to merge disturbanceActivity keys with DistActivityIDs
fullclarityactivity.keys <- clarityactivity.keys %>%
  select(ID, GlobalID) %>%
  union(rename(subset(SecchiActivityIDs, select = c(SQL.ID, SQL.GlobalID)),
               ID = SQL.ID, GlobalID = SQL.GlobalID ))

desc <- baseSD %>%
  inner_join(fullclarityactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         ClarityActivityID = ID,
         Depth_ft = DescendingDepth_ft,
         Survey123_LastEditedDate) %>%
  mutate(DepthTypeID = 2, GlobalID =paste0(GlobalID,"_2")) # Descending

asc <- baseSD %>%
  inner_join(clarityactivity.keys, by = c("parentglobalid" = "GlobalID")) %>%
  select(GlobalID = globalid,
         ClarityActivityID = ID,
         Depth_ft = AscendingDepth_ft,
         Survey123_LastEditedDate) %>%
  mutate(DepthTypeID = 3, GlobalID =paste0(GlobalID,"_3")) # Ascending)

db$SecchiDepth <- rbind(desc, asc) %>% arrange(ClarityActivityID, DepthTypeID)
### Use the SQL queries in the notes to manually update the 6 records already in the DB first
secchi.keys <- uploadData(db$SecchiDepth, "data.ClaritySecchiDepth", conn, keep.guid = TRUE)


## WaterChemistryActivity table
db$WaterChemistryActivity <- visit %>%
  inner_join(visit.keys, by = c("globalid" = "GlobalID")) %>%
  select(VisitID = ID,
         GlobalID = globalid,
         SampleCollectionMethodID = SampleMethodID,
         NumberOfBottlesFiltered = BottleCountFiltered,
         NumberOfBottlesUnfiltered = BottleCountUnfiltered,
         Notes = SampleNote) %>%
  mutate(DataProcessingLevelID = 1,
         LaboratoryID = 1)  # CCAL

chem.keys <- uploadData(db$WaterChemistryActivity, "data.WaterChemistryActivity", conn,keep.guid = TRUE)


pool::poolClose(conn)

