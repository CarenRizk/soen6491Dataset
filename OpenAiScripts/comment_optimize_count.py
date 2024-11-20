import os
import re

# Define the base path where the Java files are located
JAVA_FILES_PATH = r"..\apacheSubProjectJavaRefactored"

# Define the specific Java files to count comments for
SPECIFIC_FILES_TO_COUNT = [
    "Pipeline.java", "Trigger.java", "Window.java", "ApproximateQuantiles.java",
    "ApproximateUnique.java", "Combine.java", "TextIO.java", "Coder.java",
    "Schema.java", "PubsubIO.java", "FileIO.java", "DataflowRunner.java",
    "DirectRunner.java", "FileSystems.java", "BigtableIO.java", "SpannerIO.java",
    "Flatten.java"
]

def find_nested_file(file_name, base_path):
    """
    Search for a specific file name recursively in the base directory.
    """
    for root, _, files in os.walk(base_path):
        if file_name in files:
            return os.path.join(root, file_name)
    return None

def count_optimized_comments_in_files():
    """
    Counts how many comments start with `// Optimized by LLM:` in specified Java files.
    """
    total_count = 0
    comment_pattern = re.compile(r'^\s*// Optimized by LLM:', re.MULTILINE)

    for file_name in SPECIFIC_FILES_TO_COUNT:
        file_path = find_nested_file(file_name, JAVA_FILES_PATH)
        if not file_path:
            print(f"File not found: {file_name}")
            continue

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            matches = comment_pattern.findall(content)
            file_count = len(matches)
            total_count += file_count
            print(f"{file_name}: {file_count} comments")

    print(f"Total 'Optimized by LLM' comments: {total_count}")
    return total_count

# Call the method
if __name__ == "__main__":
    count_optimized_comments_in_files()
