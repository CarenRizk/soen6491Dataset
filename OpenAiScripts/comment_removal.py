import os
import re

def remove_commented_code(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    new_lines = []
    in_block_comment = False
    is_javadoc = False

    for line in lines:
        # Check for Javadoc comments (/** ... */)
        if line.strip().startswith("/**"):
            is_javadoc = True

        # Check for end of a Javadoc comment or block comment
        if is_javadoc and "*/" in line:
            is_javadoc = False
            new_lines.append(line)  # Keep the closing line of Javadoc
            continue

        # Skip lines if inside a regular block comment, but not Javadoc
        if in_block_comment and not is_javadoc:
            if "*/" in line:
                in_block_comment = False
            continue

        # Detect the start of a regular block comment (not Javadoc)
        if "/*" in line and not is_javadoc:
            in_block_comment = True
            continue

        # Remove single-line comments (//) that look like code
        single_line_comment = re.match(r'^\s*//', line)
        if not single_line_comment or is_javadoc:
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
