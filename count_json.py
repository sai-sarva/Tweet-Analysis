import json

def read_json_in_chunks(file_path, chunk_size=1000):
    with open(file_path, 'r', encoding='utf-8') as f:
        chunk = ""
        count = 0
        for line in f:
            #print(line)
            chunk += line
            count += 1
            if count % chunk_size == 0:
                try:
                    json.loads("[" + chunk + "]")
                    print(f"Chunk {count//chunk_size} is valid.")
                except json.JSONDecodeError as e:
                    print(f"Error in chunk {count//chunk_size}: {e}")
                chunk = ""

file_path = 'tweets_clean.json'
read_json_in_chunks(file_path)
