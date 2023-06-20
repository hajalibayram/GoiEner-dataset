###############################
##                           ##
##  USER FILES TO RAW FILES  ##
##                           ##
###############################

# CONVERT GOIENER USER DATA INTO GOIENER USABLE (TIMESTAMPED) RAW DATASETS
# Improved version that corrects duplicated dates
# Corrects Daylight Savings Time!!! (2021.11.08)

library(foreach)

#---------------------------------------------#
# FUNCTION to extract info from proper fields #
#---------------------------------------------#
align_users_dataframe <- function(dfe) {
  
  id_type <- strsplit(dfe[[1]], split="_")[[1]][1]
  # File version
  file_version <- as.numeric(strsplit(dfe[[1]], split="\\.")[[1]][2])

  # P5D
  if (id_type == "P5D") {
    date_time <- strptime(dfe[["B"]], format="%Y/%m/%d %H:%M", tz="UTC")
    input     <- as.numeric(dfe[["D"]]) / 1000
    output    <- as.numeric(dfe[["E"]]) / 1000
    acq_meth  <- 0
  # B5D, F5D, RF5D
  } else if (id_type %in% c("B5D", "F5D", "RF5D")) {
    date_time <- strptime(dfe[["B"]], format="%Y/%m/%d %H:%M", tz="UTC")
    input     <- as.numeric(dfe[["D"]]) / 1000
    output    <- as.numeric(dfe[["E"]]) / 1000
    acq_meth  <- as.numeric(dfe[["J"]])
  # P1, P1D, (P2D NO AL SER CUARTOHORARIO, Y LA MISMA INFO ESTA EN P1D)
  } else if (id_type %in% c("P1", "P1D")) { #, "P2D")) {
    date_time <- strptime(dfe[["C"]], format="%Y/%m/%d %H:%M:%S", tz="UTC")
    input     <- as.numeric(dfe[["E"]])
    output    <- as.numeric(dfe[["G"]])
    acq_meth  <- as.numeric(dfe[["U"]])
  # A5D
  } else if (id_type == "A5D") {
    date_time <- strptime(dfe[["B"]], format="%Y/%m/%d %H:%M", tz="UTC")
    input     <- as.numeric(dfe[["D"]]) / 1000
    output    <- 0
    acq_meth  <- as.numeric(dfe[["J"]])
  # F1
  } else if (id_type == "F1") {
    date_time <- strptime(dfe[["C"]], format="%Y/%m/%d %H:%M:%S", tz="UTC")
    input     <- as.numeric(dfe[["E"]])
    output    <- as.numeric(dfe[["F"]])
    acq_meth  <- as.numeric(dfe[["M"]])
  } else {
    return(NULL)
  }
  
  o <- data.frame(id_type, date_time, input, output, acq_meth, file_version, file=dfe[[1]])
  return(o)
}

#---------------------------------#
# FUNCTION to manage unique dates #
#---------------------------------#
manage_uniq_dtimes <- function(dtime, df) {
  # Index of unique date-time
  idx <- which(df[,2] == dtime)
  # Return
  data.frame(
    date_time = df[idx, 2],
    input     = df[idx, 3],
    output    = df[idx, 4]
  )
}

#------------------------------------#
# FUNCTION to manage duplicate dates #
#------------------------------------#
manage_dupe_dtimes <- function(dtime, df, id) {
  
  # Indices of repeated date-times
  df <- df[which(df[,2] == dtime),]
  
  date_time <- df$date_time[1]
  
  # log_file <- paste0(out_log, Sys.getpid(), ".log")

  # (1) SAME INPUTS AND OUTPUTS (ALL RIGHT!)
  if (length(unique(df$input)) == 1 & length(unique(df$output)) == 1) {
    input  <- df$input[1]
    output <- df$output[1]
    # write(paste(Sys.time(), "1", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
  
  # (2) SAME ID TYPE & ACQ. METHOD
  } else if (length(unique(df$id_type)) == 1 & length(unique(df$acq_meth)) == 1) {
    input  <- mean(df$input)
    output <- mean(df$output)
    # write(paste(Sys.time(), "2", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (3) SAME ID TYPE & DIFFERENT ACQ. METHOD
  } else if (length(unique(df$id_type)) == 1) {
    input  <- mean(df$input[which(df$acq_meth == min(df$acq_meth, na.rm = TRUE))])
    output <- mean(df$output[which(df$acq_meth == min(df$acq_meth, na.rm = TRUE))])
    # write(paste(Sys.time(), "3", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (4) A5D & B5D
  } else if (all(df$id_type %in% c("A5D", "B5D")) & any(df$id_type == "A5D") & any(df$id_type == "B5D")) {
    input  <- mean(df$input[df$id_type == "A5D"])
    output <- mean(df$output[df$id_type == "B5D"])
    # write(paste(Sys.time(), "4", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (5) A5D & B5D & F5D & P1D
  } else if (all(df$id_type %in% c("A5D", "B5D", "F5D", "P1D")) & any(df$id_type == "A5D") & any(df$id_type == "B5D") & any(df$id_type %in% c("F5D", "P1D"))) {
    input  <- mean(df$input[which(df$acq_meth == min(df$acq_meth, na.rm = TRUE) & df$id_type %in% c("F5D", "P1D"))])
    output <- mean(df$output[which(df$acq_meth == min(df$acq_meth, na.rm = TRUE) & df$id_type %in% c("F5D", "P1D"))])
    # write(paste(Sys.time(), "5", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (6) RF5D OVER THE REST (SINCE IT IS A CORRECTION)
  } else if (any(df$id_type == "RF5D")) {
    input <- mean(df$input[df$id_type == "RF5D"])
    output <- mean(df$output[df$id_type == "RF5D"])
    # write(paste(Sys.time(), "6", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (7) P5D OVER THE REST (SINCE IT IS "BRUTO VALIDADO")
  } else if (any(df$id_type == "P5D")) {
    input <- mean(df$input[df$id_type == "P5D"])
    output <- mean(df$output[df$id_type == "P5D"])
    # write(paste(Sys.time(), "7", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (8) (F1 & P1D) | (F5D & P1D)
  } else if (all(df$id_type %in% c("F1", "P1D")) | all(df$id_type %in% c("F5D", "P1D"))) {
    input <- mean(df$input)
    output <- mean(df$output)
    # write(paste(Sys.time(), "8", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    
  # (0) ANYTHING ELSE
  } else {
    # write(paste(Sys.time(), "0", paste(df$file, collapse=","), df$date_time[1], sep=";"), file=log_file, append=TRUE)
    return(NULL)
  }
  
  o <- data.frame(date_time, input, output)
  return(o)
}

#---------------#
# Main function #
#---------------#
usr2raw <- function(in_dir, out_dir) {
  # Get all filenames in folders
  u_fnames <- list.files(in_dir, pattern="^[[:xdigit:]]")
  head(u_fnames)
  r_fnames <- list.files(out_dir, pattern="^[[:xdigit:]]")
  head(r_fnames)
  
  # IN CASE OF STOPPING THE PROCESS, IT CAN BE RETAKEN
  fnames <- setdiff(u_fnames, r_fnames)
  if (length(fnames) == 0) {
    cat("All done!/n")
  } else {
  
    # Setup parallel backend to use many processors
    cores <- parallel::detectCores() - 1
    cl <- parallel::makeCluster(cores, outfile = "")
    doParallel::registerDoParallel(cl)
    
    # Progress bar
    pb <- txtProgressBar(style=3)
    # fnames length
    length_fnames <- length(fnames)
  
    export_funs <- c(
      "align_users_dataframe",
      "manage_uniq_dtimes",
      "manage_dupe_dtimes",
      "out_log"
    )
    
    # Given a file name, return dates, inputs and outputs
    out <- foreach (x = 1:length_fnames, .export=export_funs) %dopar% {
    #for (x in 1:length_fnames) {

      # Set progress bar
      setTxtProgressBar(pb, x/length_fnames)
      # Read file
      df <- data.frame(
        data.table::fread(
          file       = paste0(in_dir, fnames[x]),
          header     = FALSE,
          sep        = ",",
          na.strings = NULL,
          fill       = TRUE,
          select     = c(1, 3:8, 11, 14, 22)
        )
      )
      
      # Provide proper column names
      colnames(df)<- c("original_file", LETTERS[c(3:8, 11, 14, 22)-1])
      
      # Align the dataframe
      df <- apply(df, 1, align_users_dataframe) #, simplify=TRUE)
      df <- as.data.frame(data.table::rbindlist(df))
      
    
      if (nrow(df) > 0) {
        # Identify repeated date-times
        dtimes <- as.data.frame(table(df[[2]]))
        uniq_dtimes <- as.POSIXct(dtimes[dtimes[,2] == 1, 1], tz="UTC")
        dupe_dtimes <- as.POSIXct(dtimes[dtimes[,2] > 1, 1], tz="UTC")
  
        # Manage duplicate dates
        if (length(dupe_dtimes) > 0) {
          dupe_df <- lapply(dupe_dtimes, manage_dupe_dtimes, df=df, id=fnames[x])
          dupe_df <- as.data.frame(data.table::rbindlist(dupe_df))
        } else {
          dupe_df <- NULL
        }
        
        # Manage unique dates
        if (length(uniq_dtimes) > 0) {
          uniq_df <- lapply(uniq_dtimes, manage_uniq_dtimes, df=df)
          uniq_df <- as.data.frame(data.table::rbindlist(uniq_df))
        } else {
          uniq_df <- NULL
        }

        # Final dataframe
        final_df <- rbind(uniq_df, dupe_df)
        # Sort by date
        final_df <- final_df[order(final_df$date_time),]
        # Write file
        data.table::fwrite(
          final_df,
          file       = paste0(out_dir, fnames[x]),
          row.names  = FALSE,
          col.names  = FALSE,
          sep        = ",",
          na         = "",
          dateTimeAs = "write.csv"
        )
      }
    }
    
    # Stop parallelization
    parallel::stopCluster(cl)
    cat("/n")
  }
}

#------------------------#
# USER DEFINED VARIABLES #
#------------------------#
if (.Platform$OS.type == "windows") {
  goiener_users_folder  <- "C:/Users/carlos.quesada/Documents/WHY/2023.01.10 - Ficheros paper dataset GoiEner/tiny_USERS/"
  goiener_output_folder <- "C:/Users/carlos.quesada/Documents/WHY/2023.01.10 - Ficheros paper dataset GoiEner/tiny_RAW/"
  out_log <- "C:/Users/carlos.quesada/Documents/WHY/2023.01.10 - Ficheros paper dataset GoiEner/usr2raw_"
}
if (.Platform$OS.type == "unix") {
  goiener_users_folder  <- "/home/ubuntu/carlos.quesada/disk/goi6/users/"
  goiener_output_folder <- "/home/ubuntu/carlos.quesada/disk/goi6/raw/"
  out_log <- "/home/ubuntu/carlos.quesada/disk/goi5/usr2raw_"
}

usr2raw(goiener_users_folder, goiener_output_folder)

