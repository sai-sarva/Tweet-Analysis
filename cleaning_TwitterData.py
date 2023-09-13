# Importing necessary modules
import json


# Function to clean and optimize the JSON file containing tweets
def clean_tweets_json(input_file_path, output_file_path):
    """
    Cleans the JSON file by removing lines that contain 'NumberLong' and 'ObjectId'.

    Parameters:
    input_file_path (str): The file path of the input JSON file.
    output_file_path (str): The file path where the cleaned JSON will be saved.
    """

    # Open the input JSON file in read mode ('r')
    with open(input_file_path, 'r', encoding='utf-8') as infile:

        # Open or create the output JSON file in write mode ('w')
        with open(output_file_path, 'w', encoding='utf-8') as outfile:

            # Loop through each line in the input file
            for line in infile:

                # Skip lines that contain 'NumberLong' or 'ObjectId'
                if 'NumberLong' not in line and 'ObjectId' not in line:
                    # Write the clean line to the output file
                    outfile.write(line)


# File paths
input_file_path = '10000 tweets 1.json'
output_file_path = 'tweets_clean.json'

# Call the function to clean the JSON file
clean_tweets_json(input_file_path, output_file_path)
