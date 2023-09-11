from bson import json_util
import json
import re

# Load the MongoDB Extended JSON as a string
with open('10000 tweets 1.json','r', encoding='utf-8') as f:
    data = f.read()

# Remove the MongoDB comments and blank lines
data = re.sub(r'/\*.*\*/', '', data)
data = re.sub(r'^\s*$', '', data, flags=re.MULTILINE)

# Parse the cleaned data as JSON
data = json.loads('[' + data + ']')

# Convert to standard JSON
standard_json = json_util.dumps(data, json_options=json_util.STRICT_JSON_OPTIONS)

# Save the standard JSON
with open('output.json', 'w') as f:
    f.write(standard_json)
