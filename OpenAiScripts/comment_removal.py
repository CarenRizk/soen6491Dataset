import os
import re

def remove_commented_code(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    new_lines = []
    in_block_comment = False
    is_javadoc = False

    for line in lines:
        # Check for Javadoc comment start (/**)
        if line.strip().startswith("/**"):
            is_javadoc = True

        # End of Javadoc comment (*/)
        if is_javadoc and "*/" in line:
            is_javadoc = False
            new_lines.append(line)
            continue

        # Regular block comment start (/*) but not inline comments
        if re.match(r'^\s*/\*', line) and not in_block_comment:
            if "*/" not in line:  # Only start block if it doesn't close on the same line
                in_block_comment = True
            continue  # Skip this line

        # End of regular block comment (*/)
        if in_block_comment and "*/" in line:
            in_block_comment = False
            continue

        # Ignore all lines inside a block comment
        if in_block_comment:
            continue

        # Skip single-line comments that start with // but retain lines with mixed code
        if not re.match(r'^\s*//', line) or is_javadoc:
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
