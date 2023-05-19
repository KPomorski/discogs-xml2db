# This function takes the last fully imported data dump collection and compares

import csv
import os
import sys
from docopt import docopt
from tqdm import tqdm


def compare_csv_files(previous_file, current_file, delta_file, chunk_size=10000):
    # Read the previous CSV file and store the data in a dictionary
    previous_data = {}
    with open(previous_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            previous_data[row[0]] = row[1:]  # Assuming the first column contains the unique identifier

    # Read the current CSV file and compare the data in chunks
    delta_data = []
    changed_records = 0
    with open(current_file, 'r') as file:
        reader = csv.reader(file)
        while True:
            chunk = []
            try:
                for _ in range(chunk_size):
                    row = next(reader)
                    chunk.append(row)
            except StopIteration:
                break

            # Compare the chunk data with the previous CSV file
            pbar = tqdm(chunk, desc='Comparing', unit=' rows', leave=True)
            for row in pbar:
                identifier = row[0]  # Assuming the first column contains the unique identifier
                if identifier in previous_data:
                    # Compare the data with the previous CSV file
                    previous_row = previous_data[identifier]
                    if row[1:] != previous_row:  # Assuming the data starts from the second column
                        delta_data.append(row)
                        changed_records += 1
                        break
                else:
                    delta_data.append(row)
                    changed_records += 1

            # Update the progress bar with the count of changed records
            pbar.set_postfix({'Changed': changed_records})

    # Write the delta data to the new CSV file
    with open(delta_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(delta_data)

    # Update the progress bar with the count of changed records
    pbar.set_postfix({'Changed': changed_records})

# arguments = docopt(__doc__, version='0.1')

# source = arguments['--source-dump']
# target = arguments['--target-dump']
# deltas = arguments['--delta-dump']

# for s, t, d in source, target, deltas:
#     if os.path.isfile(d):
#         print("error: There already exists a delta file for this file!")
#     elif os.path.isfile(s) & os.path.isfile(t):
#         compare_csv_files(s, t, d)
#     else:
#         print("error: '%s' or '%s' is not a readable file" % s, t)


# Example usage
compare_csv_files('/share/csv_dumps/20230201_dump/label.csv', '/share/csv_dumps/20230501_dump/label.csv', '/share/csv_dumps/20230501_delta/new_label.csv', chunk_size=100000)


