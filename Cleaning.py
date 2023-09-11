# Function to clean JSON file containing tweets
def clean_tweets_json(input_file, output_file):
    # Step 1: Remove comments, ObjectID, and NumberLong
    with open(input_file, 'r', encoding='utf-8') as f:
        with open(output_file, 'w', encoding='utf-8') as out:
            out.write('[')  # Include opening bracket
            for line in f:
                if '/*' not in line and 'NumberLong' not in line and 'ObjectId' not in line:
                    out.write(line)  # Only write lines back to file that do not contain the specified patterns
            out.write(']')  # Include closing bracket

    # # Step 2: Add commas between JSON objects
    # with open(output_file, 'r+', encoding='utf-8') as inp:
    #     text = inp.read()
    #     inp.seek(0)
    #     inp.write(text.replace('}\n\n{', '},\n{'))  # Place commas between JSON objects

    # # Step 3: Replace empty hashtags with NA
    # with open(output_file, 'r', encoding='utf-8') as file:
    #     filedata = file.read()
    # filedata = filedata.replace('\"hashtags\" : []', '\"hashtags\" : [\n\t\t{\n\t\t\t\"text\" : \"NA\"\n\t\t}\n\t]')  # Replace empty hashtags with NA
    # with open(output_file, 'w', encoding='utf-8') as file:
    #     file.write(filedata)

# Call the function to clean the JSON file
input_file = 'C:/Users/ss32309/OneDrive/PycharmProjects/Neo4j/10000 tweets 1.json'
output_file = 'C:/Users/ss32309/OneDrive/PycharmProjects/Neo4j/tweets_clean.json'
clean_tweets_json(input_file, output_file)
