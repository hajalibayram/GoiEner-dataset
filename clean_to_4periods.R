# SEPARATES THE TIME SERIES INTO 1-YEAR PERIODS
# June 22, 2023
# CARLOS QUESADA (DEUSTOTECH)

library(lubridate)
library(foreach)

separate_into_four <- function() {
  # Folder definition
  # input_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out/"
  # p1_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out_pre/"
  # p2_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out_in/"
  # p3_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out_pst/"
  # p4_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.06.20 - goi6/out_p4/"
  
  input_folder <- "/home/ubuntu/carlos.quesada/disk/goi6/clean/"
  p1_folder    <- "/home/ubuntu/carlos.quesada/disk/goi6/clean_sep/pre/"
  p2_folder    <- "/home/ubuntu/carlos.quesada/disk/goi6/clean_sep/in/"
  p3_folder    <- "/home/ubuntu/carlos.quesada/disk/goi6/clean_sep/pst/"
  p4_folder    <- "/home/ubuntu/carlos.quesada/disk/goi6/clean_sep/p4/"
  
  # Dias frontera
  time_A <- ymd_hms("2020-03-01 00:00:00")
  time_B <- ymd_hms("2021-06-01 00:00:00")
  time_C <- ymd_hms("2022-06-01 00:00:00")
  
  # Get list of filenames in dataset folder
  dset_filenames <- list.files(input_folder, pattern = "*.csv")
  length_fnames <- length(dset_filenames)
  
  # Setup parallel backend to use many processors
  cores <- parallel::detectCores() - 1
  cl <- parallel::makeCluster(cores, outfile = "")
  doParallel::registerDoParallel(cl)
  
  # Parallel loop
  # for(x in 1:length_fnames) {
  out <- foreach::foreach (x = 1:length_fnames) %dopar% {
    
    # File name selection
    dset_filename <- dset_filenames[x]
    print(dset_filename)
    # Load raw dataframe from dataset and impute
    file_path <- paste0(input_folder, dset_filename)
    # Load the dataset
    df <- data.frame(data.table::fread(file_path))
    
    # Data selection
    df_pre <- df[df$times < time_A, ]
    df_in  <- df[df$times >= time_A & df$times < time_B, ]
    df_pst <- df[df$times >= time_B & df$times < time_C, ]
    df_p4  <- df[df$times >= time_C, ]
    
    # PRE-COVID-19
    dt1 <- as.numeric(
      difftime(df_pre$times[nrow(df_pre)], df_pre$times[1], units="days") +
      as.difftime(1/24, units="days")
    )
    dt1 <- ifelse(length(dt1)==0, 0, dt1)
    if(dt1 >= 365) {
      data.table::fwrite(
        x    = df_pre,
        file = paste0(p1_folder, dset_filename),
        dateTimeAs = "write.csv"
      )
    }
    # DURING COVID-19
    dt2 <- as.numeric(
      difftime(df_in$times[nrow(df_in)], df_in$times[1], units="days") +
      as.difftime(1/24, units="days")
    )
    dt2 <- ifelse(length(dt2)==0, 0, dt2)
    if(dt2 >= 365) {
      data.table::fwrite(
        x    = df_in,
        file = paste0(p2_folder, dset_filename),
        dateTimeAs = "write.csv"
      )
    }
    # POST-COVID-19
    dt3 <- as.numeric(
      difftime(df_pst$times[nrow(df_pst)], df_pst$times[1], units="days") +
      as.difftime(1/24, units="days")
    )
    dt3 <- ifelse(length(dt3)==0, 0, dt3)
    if(dt3 >= 365) {
      data.table::fwrite(
        x    = df_pst,
        file = paste0(p3_folder, dset_filename),
        dateTimeAs = "write.csv"
      )
    }
    # TARIFF CHANGE
    dt4 <- as.numeric(
      difftime(df_p4$times[nrow(df_p4)], df_p4$times[1], units="days") +
      as.difftime(1/24, units="days")
    )
    dt4 <- ifelse(length(dt4)==0, 0, dt4)
    if(dt4 >= 365) {
      data.table::fwrite(
        x    = df_p4,
        file = paste0(p4_folder, dset_filename),
        dateTimeAs = "write.csv"
      )
    }
  }
  
  # Stop parallelization
  parallel::stopCluster(cl)
}

separate_into_four()