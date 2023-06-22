library(foreach)
library(lubridate)
library(tidyr)

################################################################################
# cook_raw_dataframe()
################################################################################

cook_raw_dataframe <- function(raw_df) {
  # List of samples per day (REMARK: ADD AS NEEDED!)
  spd <- 24
  
  # Time series ends
  from_date <- raw_df$times[1]
  to_date   <- raw_df$times[nrow(raw_df)] 

  # Round all dates towards zero according to the spd
  date_floors <- floor_date(raw_df$times, unit = paste(86400/(spd*60), "min"))
  # Aggregate according to the number of samples per day (spd)
  aggr_data <- aggregate(
    x   = raw_df$values,
    by  = list(times = date_floors),
    FUN = sum
  )
  # Create time sequence
  time_seq <- seq(
    from = min(date_floors),
    to = max(date_floors),
    by = paste(86400/(spd*60), "min")
  )
  # Complete data frame
  cooked_df <- as.data.frame(tidyr::complete(aggr_data, times=time_seq))
  colnames(cooked_df)<- c("times", "values")
  
  return(cooked_df)
}

################################################################################
# impute_cooked_dataframe()
################################################################################

impute_cooked_dataframe <- function(cdf, season, short_gap) {
  # Initialize
  imp_ts <- NULL
  
  # Time series pending imputation
  not_imp_ts <- ts(data=cdf[,2], frequency=season) # 1 week
  
  # Imputed time series
  try(
    imp_ts <- imputeTS::na_interpolation(
      not_imp_ts,
      maxgap = short_gap
    ),
    silent=TRUE
  )
  try(
    imp_ts <- imputeTS::na_seasplit(
      imp_ts,
      algorithm = "locf"
    ),
    silent=TRUE
  )
  
  if(!is.null(imp_ts)) {
    # Imputed dataframe
    cdf <- data.frame(
      times   = cdf[,1],
      values  = as.double(imp_ts),
      imputed = as.integer(is.na(not_imp_ts))
    )
  } else {
    cdf <- NULL
  }
  return(cdf)
}

################################################################################
# extend_dataset_v2()
################################################################################

extend_dataset_v2 <- function(
  input_folder,
  output_folder,
  min_years = 0
  ) {
  
  # Get list of filenames in dataset folder
  dset_filenames <- list.files(input_folder, pattern = "*.csv")

  # Setup parallel backend to use many processors
  cores <- parallel::detectCores() - 1
  cl <- parallel::makeCluster(cores, outfile = "")
  doParallel::registerDoParallel(cl)
  
  # fnames length
  length_fnames <- length(dset_filenames)
  
  # Analysis loop
  packages <- c("tidyr", "lubridate")
  export <- c(
    "cook_raw_dataframe",
    "impute_cooked_dataframe"
  )
  
  out <- foreach::foreach (
    x = 1:length_fnames,
    .packages = packages,
    .export = export
  ) %dopar% {
  # for(x in 1:length_fnames) {
    
    # File name selection
    dset_filename <- dset_filenames[x]
    # Load raw dataframe from dataset and impute
    file_path <- paste0(input_folder, dset_filename)
    
    # Load the dataset (ONLY CONSUMPTION)
    rdf <- data.frame(data.table::fread(file_path, select=1:2))
    colnames(rdf) <- c("times", "values")
    
	  ### HERE IS THE tidyr::complete() FUNCTION
    rdf <- cook_raw_dataframe(raw_df = rdf)
  
    # IMPUTA
    if (!is.null(rdf)) {
      rdf <- impute_cooked_dataframe(
        cdf       = rdf, 
        season    = 168, 
        short_gap = 8
      )
    }
    
    # Escribe los ficheros en la carpeta de salida
    data.table::fwrite(
      x    = rdf,
      file = paste0(output_folder, dset_filename),
      dateTimeAs = "write.csv"
    )
    
  }
  # Stop parallelization
  parallel::stopCluster(cl)
}

### USER DEFINED VARIABLES

#Function call for "goi"
extend_dataset_v2(
  input_folder  = "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/in/",
  output_folder = "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out/"
)