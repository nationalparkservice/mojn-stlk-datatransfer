library(reticulate)
library(jsonlite)
library(httr)
library(tidyverse)

# Download photos using Python script
gdb.path <- "M:/MONITORING/StreamsLakes/Data/WY2019/FieldData/Lakes_Annual/STLK_AnnualLakeVisit_20191022.gdb"
photo.table <- paste(gdb.path, "Photos__ATTACH", sep = "/")
visit.data <- paste(gdb.path, "STLK_Lake_Annual_Field_Visit", sep = "/")
photo.data <- paste(gdb.path, "Photos", sep = "/")
photo.dest <- "M:/MONITORING/StreamsLakes/Data/WY2019/ImageData/Lakes"
originals.dest <- "M:/MONITORING/_FieldPhotoOriginals_DoNotModify/AGOL_STLK"
# photo.dest <- "C:/Users/sewright/Desktop/STLKPhotoDownloadTest"
# originals.dest <- "C:/Users/sewright/Desktop/STLKPhotoDownloadTest/Originals"
source_python("download-photos.py")

## Download photos from annual lake visits
download_visit_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest)

## Download benchmark photos
gdb.path <- "M:/MONITORING/StreamsLakes/Data/WY2019/FieldData/Lakes_Annual/STLK_LakeLevels_20191022.gdb"
photo.table <- paste(gdb.path, "BenchPhoto__ATTACH", sep = "/")
visit.data <- paste(gdb.path, "Form_2", sep = "/")
photo.data <- paste(gdb.path, "BenchPhoto", sep = "/")

download_bench_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest)

# Read tabular data from AGOL

## Get a token with a headless account
token_resp <- POST("https://nps.maps.arcgis.com/sharing/rest/generateToken",
                   body = list(username = rstudioapi::showPrompt("Username", "Please enter your AGOL username", default = "mojn_hydro"),
                               password = rstudioapi::askForPassword("Please enter your AGOL password"),
                               referer = 'https://irma.nps.gov',
                               f = 'json'),
                   encode = "form")
agol_token <- fromJSON(content(token_resp, type="text", encoding = "UTF-8"))

## Get annual lake visit data
resp.visit <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/0/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
visit <- fromJSON(content(resp.visit, type = "text", encoding = "UTF-8"))
visit <- visit$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(StartTime = as.POSIXct(StartTime/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = StartTime)

resp.dl <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/1/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
sensor.dl <- fromJSON(content(resp.dl, type = "text", encoding = "UTF-8"))
sensor.dl <- sensor.dl$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.deploy <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/2/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
sensor.deploy <- fromJSON(content(resp.deploy, type = "text", encoding = "UTF-8"))
sensor.deploy <- cbind(sensor.deploy$features$attributes, sensor.deploy$features$geometry) %>%
  mutate(wkid = sensor.deploy$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.photos <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/3/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
photos <- fromJSON(content(resp.photos, type = "text", encoding = "UTF-8"))
photos <- photos$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.crew <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/4/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
crew <- fromJSON(content(resp.crew, type = "text", encoding = "UTF-8"))
crew <- crew$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.wq <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/5/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
wq <- fromJSON(content(resp.wq, type = "text", encoding = "UTF-8"))
wq <- wq$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.secchi <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/6/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
secchi <- fromJSON(content(resp.secchi, type = "text", encoding = "UTF-8"))
secchi <- secchi$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.sample <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/7/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
sample <- fromJSON(content(resp.sample, type = "text", encoding = "UTF-8"))
sample <- sample$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

## Get lake levels data
resp.levels <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_e2571ff8454a4c65900a22297d10841f/FeatureServer/0/query",
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
levels <- fromJSON(content(resp.levels, type = "text", encoding = "UTF-8"))
levels <- levels$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(SampleDate = as.POSIXct(SampleDate/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = SampleDate)

resp.levels.crew <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_e2571ff8454a4c65900a22297d10841f/FeatureServer/1/query",
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))
levels.crew <- fromJSON(content(resp.levels.crew, type = "text", encoding = "UTF-8"))
levels.crew <- levels.crew$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.benchphoto <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_e2571ff8454a4c65900a22297d10841f/FeatureServer/2/query",
                       query = list(where="1=1",
                                    outFields="*",
                                    f="JSON",
                                    token=agol_token$token))
bench.photo <- fromJSON(content(resp.benchphoto, type = "text", encoding = "UTF-8"))
bench.photo <- bench.photo$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

# Wrangle data

## Get Site table from database
params <- readr::read_csv("C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv") %>%  # TODO: Change to real database connection after testing is done
  as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)
sites <- dplyr::tbl(conn, dbplyr::in_schema("data", "Site")) %>%
  dplyr::collect()
pool::poolClose(conn)

## Visit table
db <- list()
db$Visit <- visit %>%
  select(LakeCode, StartDateTime, Notes = OverallNotes) %>%
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
  rename(SiteID = ID)

# Load data into SQL database
