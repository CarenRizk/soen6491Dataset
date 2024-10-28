import os
import re

def remove_commented_code(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    new_lines = []
    in_block_comment = False
    for line in lines:
        # Check for block comments (/* */)
        if in_block_comment:
            if '*/' in line:
                in_block_comment = False
            continue  # Skip line if inside block comment
        if '/*' in line:
            in_block_comment = True
            continue  # Start of a block comment, skip line

        # Remove single-line comments (//) that look like code (ignoring javadoc comments and TODOs)
        single_line_comment = re.match(r'^\s*//', line)
        if not single_line_comment:
            new_lines.append(line)

    # Write the cleaned lines back to the file
    with open(file_path, 'w') as file:
        file.writelines(new_lines)

def remove_comments_in_project(project_path):
    for root, dirs, files in os.walk(project_path):
        for file in files:
            if file.endswith(".java"):
                file_path = os.path.join(root, file)
                remove_commented_code(file_path)
                print(f"Processed {file_path}")

# Run the script on your Java project directory
project_path = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectJava\src"  # Adjust this path to your project
remove_comments_in_project(project_path)
