import os
import re

# Specify your project path
project_path = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectJava\src"

# Regular expression to match the Javadoc comments
javadoc_comment_pattern = re.compile(
    r"/\*\*.*?\*/", re.DOTALL
)

def remove_javadoc_comments(file_path):
    """Remove Javadoc comments from a Java file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Remove Javadoc comments
    modified_content = javadoc_comment_pattern.sub('', content)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

def process_directory(directory):
    """Traverse the directory and process each Java file."""
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.java'):
                file_path = os.path.join(root, filename)
                remove_javadoc_comments(file_path)
                print(f"Processed: {file_path}")

# Process the project directory
process_directory(project_path)
