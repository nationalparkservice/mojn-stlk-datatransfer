library(reticulate)

# Download photos using Python script
gdb.path <- "M:/MONITORING/StreamsLakes/Data/WY2019/FieldData/Lakes_Annual/STLK_AnnualLakeVisit_20191022.gdb"
photo.table <- paste(gdb.path, "Photos__ATTACH", sep = "/")
visit.data <- paste(gdb.path, "STLK_Lake_Annual_Field_Visit", sep = "/")
photo.data <- paste(gdb.path, "Photos", sep = "/")
photo.dest <- "M:/MONITORING/StreamsLakes/Data/WY2019/ImageData/Lakes"
originals.dest <- "M:/MONITORING/_FieldPhotoOriginals_DoNotModify/AGOL_STLK"
source_python("download-photos.py")

download_photos(attTable = photo.table, photoFeatureClass = photo.data, visitFeatureClass = visit.data, dataPhotoLocation = photo.dest, originalsLocation = originals.dest)

# Read tabular data from AGOL

# Wrangle data

# Load data into SQL database