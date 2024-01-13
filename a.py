import json

# Load the JSON data
input_file = 'input.json'  # Replace with your input file name
output_file = 'output.json'

with open(input_file, 'r') as file:
    data = json.load(file)

# Function to add properties
def add_properties(layouts):
    for row in layouts:
        for well in row:
            well_data = well["container/well/fixed-plate-well/*/"]["json_addl"]
            name = well_data["cont_address"]["name"]
            well_data["properties"] = {"name": name}

# Apply the function to both fixed-plate-24 and fixed-plate-96
add_properties(data["fixed-plate-24"]["1.0"]["instantiation_layouts"])
add_properties(data["fixed-plate-96"]["1.0"]["instantiation_layouts"])

# Write the modified data back to a new JSON file
with open(output_file, 'w') as file:
    json.dump(data, file, indent=4)

print(f"Modified JSON saved to {output_file}")
