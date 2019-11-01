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
