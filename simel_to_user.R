#################################
##                             ##
##  SIMEL FILES TO USER FILES  ##
##                             ##
#################################

# Given a folder with SIMEL files, this script generates a file per user.
# This version incorporates parallelization.

library(foreach)

### USER DEFINED VARIABLES
if (.Platform$OS.type == "windows") {
  g_input  <- "C:/Users/carlos.quesada/Documents/WHY/2023.01.10 - Ficheros paper dataset GoiEner/tiny_SIMEL/"
  g_output <- "C:/Users/carlos.quesada/Documents/WHY/2023.01.10 - Ficheros paper dataset GoiEner/tiny_USERS/"
}
if (.Platform$OS.type == "unix") {
  g_input <- "/home/ubuntu/carlos.quesada/disk/goi5/simel/"
  g_output <- "/home/ubuntu/carlos.quesada/disk/goi5/users/"
}

# Get list of filenames in dataset folder
filenames <- list.files(g_input)
total_fnames <- length(filenames)

# Setup parallel backend to use many processors
cores <- parallel::detectCores() - 1
cl <- parallel::makeCluster(cores, outfile = "")
doParallel::registerDoParallel(cl)

# Progress bar
pb <- txtProgressBar(style=3)
# File by file
out <- foreach::foreach (ff = 1:total_fnames) %dopar% {
  # Get filename
  filename <- filenames[ff]
  # Set progress bar
  setTxtProgressBar(pb, ff/total_fnames)

  # Load Goiener SIMEL file
  simel_df <- data.frame(
    data.table::fread(
      file       = paste0(g_input, filename),
      header     = FALSE,
      sep        = ";",
      na.strings = ""
    )
  )
  # Unique users in file
  unique_users <- unique(simel_df$V1)
  for (uu in 1:length(unique_users)) {
    particular_user_id <- unique_users[uu]
    # Select all particular_user_id entries in df
    all_user_rows <- simel_df$V1 == particular_user_id
    # Create new dataframe
    user_df <- data.frame(filename, simel_df[all_user_rows, ])
    # Complete columns (to 24) with NA values
    user_df[,(ncol(user_df)+1):24] <- NA
    # Get only the files of interest
    id_type <- sapply(strsplit(user_df$filename, split="_"), "[[", 1)
    user_df <- user_df[id_type %in% c("A5D", "B5D", "F1", "F5D", "P1", "P1D", "P5D", "RF5D"),]
    if (nrow(user_df) > 0) {
      # Save
      data.table::fwrite(
        x         = user_df,
        file      = paste0(g_output, particular_user_id, ".csv"),
        append    = TRUE,
        quote     = FALSE,
        sep       = ",",
        row.names = FALSE,
        col.names = FALSE,
        na        = ""
      )
    }
  }
}

# Stop parallelization
parallel::stopCluster(cl)

cat("\n")