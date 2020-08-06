## This script is written specifically to update existing chem data from the corrected lab data found here: N:\STLK\Deliveries\OSU_AlkalinityCorrection_2020
## Once we work with CCAL to develop a consistent format for lab data exports (incl. site code and visit type), this script should be generalized
## to work with regular annual lab data.

library(tidyverse)
library(magrittr)
library(readxl)

#------------Open Database Connection---------------#
db.params.path <- "C:/Users/sewright/Documents/R/mojn-stlk-datatransfer/stlk-database-conn.csv"

db <- list()

## Get Site table from database
params <- readr::read_csv(db.params.path) %>%  # TODO: Change to real database connection after testing is done
	as.list()
params$drv <- odbc::odbc()
conn <- do.call(pool::dbPool, params)
#---------------------------#

#------------Data Wrangling---------------#
# dir <- file.path("N:", "STLK", "Deliveries", "OSU_AlkalinityCorrection_2020")
dir <- file.path(".", "data", "OSU_AlkalinityCorrection_2020")
file_names <- c("MOJN_101619_alk rev.xlsx",
							 "MOJN_101818_rev2 alk.xlsx",
							 "MOJN_102115_alk rev.xlsx",
							 "MOJN_103117_rev alk.xlsx",
							 "MOJN_120116_alk rev.xlsx")
file_paths <- file.path(dir, file_names)

tidy_chem <- function(df) {
	metadata_cols <- c("SampleName", "Project Code", "Lab Number", "Site ID", "Remark", "Delivery Date", "Thawing Date", "Comment")
	names(df) <- trimws(names(df), which = "both")
	names(df)[1] <- "SampleName"
	
	long_df <- select(df, any_of(metadata_cols))  # Use any_of since thawing data isn't present in every dataset
	data_start <- sum(names(long_df) != "Comment") + 1  # Index of first data column. Don't count Comment column since it's the last column
	data_end <- ncol(df) - 1 - sum(names(long_df) == "Comment")
	
	long_df <- cbind(long_df, select(df, c(data_start + 1, data_start)))
	# names(long_df)[data_start + 1] <- "Date"
	
	long_df %<>% pivot_longer(cols = -any_of(c(metadata_cols, "Date")), names_to = "Parameter", values_to = "LabValue")
	
	data_start <- data_start + 2
	for (i in seq(data_start, data_end, 2)) {
		date_col <- i + 1
		data_col <- i
		col_names <- names(df)[c(date_col, data_col)]
		
		temp_df <- select(df, any_of(c(metadata_cols, col_names)))
		names(temp_df)[length(names(temp_df)) - 1] <- "Date"
		temp_df %<>% pivot_longer(cols = -any_of(c(metadata_cols, "Date")), names_to = "Parameter", values_to = "LabValue") %>%
			filter(!is.na(LabValue))
		
		long_df <- rbind(long_df, temp_df)
	}
	
	return(long_df)
}

chem_data <- sapply(file_paths, read_xlsx, sheet = "MOJN Data", skip = 3, trim_ws = TRUE, col_names = TRUE, .name_repair = "minimal")
chem_data_long <- sapply(chem_data, tidy_chem) %>%
	bind_rows()

# Primary keys for sample type (regular vs. duplicate)
regular_id <- 1
duplicate_id <- 4
triplicate_id <- 5

# Primary keys for data quality flag
flag_none <- 1
flag_info <- 2

chem_data_upload <- chem_data_long %>%
	mutate(SampleTypeID = if_else(grepl("Duplicate", Parameter), true = duplicate_id, false = if_else(grepl("Triplicate", Parameter), true = triplicate_id, false = regular_id))) %>%
	mutate(Parameter = str_remove(Parameter, "Duplicate\\s|Triplicate\\s"),
				 DataQualityFlag = if_else(grepl("\\*", LabValue), true = flag_info, false = flag_none),
				 LabValue = str_remove(LabValue, "\\*")) %>%
	separate(Parameter, into = c("Characteristic", "Units"), sep = "\\(|\\)") %>%
	mutate(Characteristic = trimws(Characteristic, which = "both"))
	
#---------------------------#

#-------------Read & inspect data from Database--------------#

db_chem <- dplyr::tbl(conn, dbplyr::in_schema("data", "WaterChemistryLabResult")) %>%
	dplyr::collect() %>%
	dplyr::mutate_if(is.character, trimws)

db_chem_activity <- dplyr::tbl(conn, dbplyr::in_schema("data", "WaterChemistryActivity")) %>%
	dplyr::collect() %>%
	dplyr::mutate_if(is.character, trimws)

db_chem_params <- dplyr::tbl(conn, dbplyr::in_schema("ref", "WaterCharacteristic")) %>%
	dplyr::collect() %>%
	dplyr::mutate_if(is.character, trimws)

db_chem_params %<>% arrange(Code)
db_chem_params$LabCode <- c("", "Alkalinity", "", "Ca", "Cl", "Conductivity", "DOC", "K", "Mg", "Na", "NO3-N+NO2-N", "pH", "SO4-S", "TDN", "TDP", "UTN", "UTP")

# Get lab sample number into db_chem
all_chem <- left_join(db_chem, 
											select(db_chem_activity, ID, LabSampleNumber),
											by = c("WaterChemistryActivityID" = "ID"))

# Get lab parameter code into db_chem
all_chem <- left_join(all_chem,
											select(db_chem_params, ID, LabCode, Code),
											by = c("WaterCharacteristicID" = "ID"))
# Join chem_data_upload to db_chem
all_chem <- left_join(all_chem,
											select(chem_data_upload, `Lab Number`, `SampleTypeID`, `Characteristic`, `LabValue`, `Site ID`),
											by = c("LabSampleNumber" = "Lab Number", "SampleTypeID" = "SampleTypeID", "LabCode" = "Characteristic"))

#---------------------------#

#-------------Update Data in Database--------------#
chem_to_update <- filter(all_chem, round(as.numeric(LabValue.x), 4) != round(as.numeric(LabValue.y), 4))

qry <- paste0("UPDATE data.WaterChemistryLabResult ",
							"SET LabValue = ? ",
							"WHERE ID = #;")
update_qry <- ""

for (i in 1:nrow(chem_to_update)) {
	data_row <- chem_to_update[i,]
	qry_string <- str_replace(qry, "\\?", as.character(data_row['LabValue.y']))
	qry_string <- str_replace(qry_string, "\\#", as.character(data_row['ID']))
	update_qry <- paste(update_qry, qry_string)
}

pool::poolWithTransaction(pool = conn, func = function(conn) {
	
	res <- DBI::dbSendStatement(conn, update_qry)
	rows_affected <- DBI::dbGetRowsAffected(res)
})
	
rows_affected
#---------------------------#

#------------Close Database Connection---------------#
pool::poolClose(conn)
#---------------------------#