#---------Settings----------#
# gdb.path <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\FieldData\\Lakes_Annual\\STLK_AnnualLakeVisit_20191022.gdb"
gdb.path <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\MOJN_STLK_AnnualLakeVisit_20210224.gdb"
# photo.dest <- "M:\\MONITORING\\StreamsLakes\\Data\\WY2019\\ImageData\\Lakes"
# originals.dest <- "M:\\MONITORING\\_FieldPhotoOriginals_DoNotModify\\AGOL_STLK"
# db.params.path <- "C:\\Users\\EEdson\\Desktop\\Projects\\MOJN\\stlk-database-conn.csv"
photo.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest"
originals.dest <- "C:\\Users\\sewright\\Desktop\\STLKPhotoDownloadTest\\Originals"
db.params.path <- "C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv"
#---------------------------#

Sys.setenv(RETICULATE_PYTHON = "C:/Python27/ArcGISx6410.5")
library(reticulate)
library(jsonlite)
library(httr)
library(tidyverse)
library(pool)
library(dbplyr)
library(DBI)
source("utils_expanded.R")

source("agol-download.R")
source("data-upload.R")
