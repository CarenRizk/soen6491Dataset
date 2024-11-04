import os
import re

# Specify your project path
project_path = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectJava\src"

# Regular expressions to match the Javadoc and single-line comments
javadoc_comment_pattern = re.compile(
    r"/\*\*.*?\*/", re.DOTALL  # Match Javadoc comments
)

single_line_comment_pattern = re.compile(
    r"//.*?$",
    re.MULTILINE  # Match single-line comments
)

def remove_comments(file_path):
    """Remove Javadoc and single-line comments from a Java file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Remove Javadoc comments
    modified_content = javadoc_comment_pattern.sub('', content)

    # Remove single-line comments
    modified_content = single_line_comment_pattern.sub('', modified_content)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

def process_directory(directory):
    """Traverse the directory and process each Java file."""
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.java'):
                file_path = os.path.join(root, filename)
                remove_comments(file_path)
                print(f"Processed: {file_path}")

# Process the project directory
process_directory(project_path)
