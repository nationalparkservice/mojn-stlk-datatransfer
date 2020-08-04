## This script is written specifically to update existing chem data from the corrected lab data found here: N:\STLK\Deliveries\OSU_AlkalinityCorrection_2020
## Once we work with CCAL to develop a consistent format for lab data exports (incl. site code and visit type), this script should be generalized
## to work with regular annual lab data.

library(tidyverse)
library(magrittr)
library(readxl)

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
				 DataQualityFlag = flag_none) %>%
	separate(Parameter, into = c("Characteristic", "Units"), sep = "\\(|\\)")
	
