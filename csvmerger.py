import csv
import glob

class CSVMerger:
    def __init__(self, source_folder, output_file):
        self._source_folder = source_folder
        self._output_file = output_file


    def combine_csv_files(self):
        # Get a list of all .csv files in the source folder
        csv_files = glob.glob(f"{self._source_folder}/*.csv")

        # Open the output file in write mode
        with open(self._output_file, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)

            # Variable to track if headers have been written
            headers_written = False

            # Loop through each .csv file
            for file_path in csv_files:

                # Open each CSV file in read mode
                with open(file_path, mode='r', newline='') as infile:
                    reader = csv.reader(infile)

                    # Write headers only once
                    if not headers_written:
                        headers = next(reader)  # Read the header of the first file
                        writer.writerow(headers)  # Write the header to the output file
                        headers_written = True
                    else:
                        next(reader)  # Skip the header of subsequent files

                    # Write the data rows to the output file
                    for row in reader:
                        writer.writerow(row)
