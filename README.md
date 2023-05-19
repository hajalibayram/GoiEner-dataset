# GoiEner-dataset
GoiEner dataset processing files

# GoiEner Data Processing and Cleaning

This repository contains scripts for processing and cleaning GoiEner's smart meter data. It contains three R scripts named `simel_to_user.R`, `user_to_raw.R` and `raw_to_clean.R`. These scripts convert SIMEL files to USER files, USER files to RAW files (raw electricity usage CSV files), and RAW files to CLEAN files (clean electricity usage CSV files before being split into pre-, during, and post-COVID-19 files), respectively.

## Getting Started

### Prerequisites

* R software and environment

## Repository Files

* `simel_to_user.R`: This script transforms SIMEL files from the directory indicated by the variable `g_input` into USER files in the directory indicated by the variable `g_output`.

* `user_to_raw.R`: This script takes all USER files from the directory indicated by the variable `goiener_users_folder`, processes them, and transforms them into RAW files. The resulting RAW files are saved in the directory indicated by the variable `goiener_output_folder`.

* `raw_to_clean.R`: This script transforms RAW files from the directory indicated by the variable `input_folder` into CLEAN files. The CLEAN files are stored in the directory indicated by the variable `output_folder`.

## Workflow

1. **From SIMEL files to USER files (CUPS files)**: The `simel_to_user.R` script collects entries related to each anonymized CUPS from all SIMEL files, and generates individual USER files for each CUPS.

2. **From USER files (CUPS files) to RAW files**: The `user_to_raw.R` script eliminates duplicate entries in each USER file that have the same timestamp. This step ensures the data is accurate and consistent.In addition, it processes the USER files to generate the RAW files in CSV format, extracting and organizing relevant information. 

3. **Data Cleaning (From RAW files to CLEAN files)**: The `raw_to_clean.R` script cleans the raw data in the RAW files and transforms them into CLEAN files. It imputes missing data, adjusts timestamps to local time, avoids COVID-19 lockdown dates, excludes short load profiles, and excludes 0-valued load profiles.

## Additional Data

The original SIMEL files provided by GoiEner are available on Zenodo in the ["GoiEner smart meters raw data"](https://zenodo.org/record/7859413) repository. The processed data are also available in the ["GoiEner smart meters data"](https://zenodo.org/record/7362094) repository on Zenodo.


## Contact

For any queries or issues, please contact carlos.quesada@deusto.es
