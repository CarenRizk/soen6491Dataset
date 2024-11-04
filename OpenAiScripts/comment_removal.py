import re

def remove_javadoc_comments(input_file, output_file):
    # Regular expression pattern for JavaDoc comments
    javadoc_pattern = r'/\*\*.*?\*/'

    # Read the content of the input file
    with open(input_file, 'r', encoding='utf-8') as file:
        content = file.read()

    # Remove JavaDoc comments using regex
    cleaned_content = re.sub(javadoc_pattern, '', content, flags=re.DOTALL)

    # Write the cleaned content to the output file
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(cleaned_content)

    print(f"Removed JavaDoc comments from '{input_file}' and saved to '{output_file}'.")

# Usage example
input_file_path = 'path/to/your/JavaFile.java'  # Change this to your input file path
output_file_path = 'path/to/your/CleanedJavaFile.java'  # Change this to your desired output file path

remove_javadoc_comments(input_file_path, output_file_path)
