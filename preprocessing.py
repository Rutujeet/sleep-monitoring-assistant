import os
import pandas as pd

# Step 1: Data Extraction
def extract_data(file_path):
    data = pd.read_csv(file_path, sep=",", header=None)
    return data

# Step 2: Data Transformation
def transform_data(data):
    print(data.head())
    data.columns = ["timestamp", "heart_rate"]  # rename columns
    data = data.dropna()  # remove missing values
    data = data.drop_duplicates()  # remove duplicates
    return data

# Step 3: Data Loading
def load_data(data, file_path):
    data.to_csv(file_path, index=False)

# Create the data pipeline
def data_pipeline(input_folder, output_folder):
    # Walk through the input folder
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            # Check if the file is a heart rate data file
            if file.endswith(".txt"):
                # Extract
                data = extract_data(os.path.join(root, file))
                
                # Transform
                data = transform_data(data)
                
                # Load
                # Create the output file path by replacing the input folder path with the output folder path
                output_file_path = os.path.join(output_folder, os.path.relpath(root, input_folder), file.replace(".txt", ".csv"))
                
                # Create the output directory if it doesn't exist
                os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
                
                load_data(data, output_file_path)

# Define the input and output folders
input_folder = "Add path to input folder"
output_folder = "Add path to output folder"

# Run the data pipeline
data_pipeline(input_folder, output_folder)
