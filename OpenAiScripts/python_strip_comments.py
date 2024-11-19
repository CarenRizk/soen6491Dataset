import re
import os

project_path = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectPython"


def remove_comments(file_path):
    print(file_path)
    with open(file_path, 'r') as file:
        lines = file.readlines()

    stripped_lines = [line for line in lines if not line.strip().startswith('#')]

    with open(file_path, 'w') as file:
        file.writelines(stripped_lines)

def process_directory(directory):
    # Only process files in the given directory, not subdirectories
    for filename in os.listdir(directory):
        if filename == "textio_test.py":
            file_path = os.path.join(directory, filename)
            # Check if it's a Python file and not a directory
            if os.path.isfile(file_path) and filename.endswith('.py'):
                remove_comments(file_path)
                print(f"Processed: {file_path}")

# Process the project directory
process_directory(project_path)