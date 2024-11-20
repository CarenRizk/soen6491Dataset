import os
import logging
import time
import random
from openai import OpenAI
import re

# Configure OpenAI API
api_key = ''
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
TEMPERATURE = 0.2
TOKEN_LIMIT = 12000
MAX_LIMIT = 16100
MODEL = "gpt-4o-mini"
REFACTORED_PATH = r"..\apacheSubProjectJavaRefactored"
PYTHON_OUTPUT_PATH = r"..\apacheSubProjectPythonTranslated"
PYTHON_PATH = r"..\apacheSubProjectPython"
SPECIFIC_FILES_TO_TRANSLATE = [
    ["Pipeline.java", "PipelineTest.java"],
    ["Trigger.java", "TriggerTest.java"],
    ["Window.java", "WindowingTest.java"],
    ["ApproximateQuantiles.java", "ApproximateQuantilesTest.java"],
    ["ApproximateUnique.java", "ApproximateUniqueTest.java"],
    ["Combine.java", "CombineTest.java"],
    ["TextIO.java", "TextIOReadTest.java", "TextIOWriteTest.java"],
    ["Coder.java", "CoderTest.java"],
    ["Schema.java", "SchemaTest.java"],
    ["PubsubIO.java", "PubsubIOTest.java"],
    ["FileIO.java", "FileIOTest.java"],
    ["DataflowRunner.java", "DataflowRunnerTest.java"],
    ["DirectRunner.java", "DirectRunnerTest.java"],
    ["FileSystems.java", "FileSystemsTest.java"],
    ["BigtableIO.java", "BigtableIOTest.java"],
    ["SpannerIO.java", "SpannerIOWriteTest.java", "SpannerIOReadTest.java", "SpannerIOReadChangeStreamTest.java"],
    ["Flatten.java", "FlattenTest.java"]
]

PYTHON_FILE_MAPPING = {
    "Pipeline.java": ["pipeline.py", "pipeline_test.py"],
    "Trigger.java": ["trigger.py", "trigger_test.py"],
    "Window.java": ["window.py", "window_test.py"],
    "ApproximateQuantiles.java": ["approximatequantiles.py", "approximatequantiles_test.py"],
    "ApproximateUnique.java": ["approximateunique.py", "approximateunique_test.py"],
    "Combine.java": ["combiners.py", "combiners_test.py"],
    "TextIO.java": ["textio.py", "textio_test.py"],
    "Coder.java": ["coders.py", "coders_test.py"],
    "Schema.java": ["schemas.py", "schemas_test.py"],
    "PubsubIO.java": ["pubsub.py", "pubsub_test.py"],
    "FileIO.java": ["fileio.py", "fileio_test.py"],
    "DataflowRunner.java": ["dataflow_runner.py", "dataflow_runner_test.py"],
    "DirectRunner.java": ["direct_runner.py", "direct_runner_test.py"],
    "FileSystems.java": ["filesystems.py", "filesystems_test.py"],
    "BigtableIO.java": ["bigtableio.py", "bigtableio_test.py"],
    "SpannerIO.java": ["spannerio.py", "spannerio_test.py"],
    "Flatten.java": ["flatten.py", "flatten_test.py"],
}


# Create output directory if it doesn't exist
os.makedirs(PYTHON_OUTPUT_PATH, exist_ok=True)

def chatgpt_response_with_backoff(**kwargs):
    client = OpenAI(api_key=api_key)
    return client.chat.completions.create(**kwargs)

def extract_imports_from_file(file_path):
    imports = set()
    import_pattern = re.compile(r'^\s*(import .+|from .+ import .+)')

    if not file_path or not os.path.isfile(file_path):
        logging.warning(f"No valid Python file found at {file_path}")
        return []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            match = import_pattern.match(line)
            if match:
                imports.add(match.group(0).strip())

    return sorted(imports)

def extract_relevant_sections(python_code, max_lines=200):
    """
    Extracts the first few lines of the Python reference code, including imports and the beginning of the main logic.
    """
    lines = python_code.strip().split('\n')
    return "\n".join(lines[:max_lines])

def translate_to_python(java_code, allowed_imports, python_reference):
    allowed_imports_text = "\n".join(allowed_imports)

    if isinstance(python_reference, list):
        python_reference = "\n".join(python_reference)

    relevant_python_reference = extract_relevant_sections(python_reference)

    prompt = f"""
    Translate the following Java code into Python using the Apache Beam Python SDK.
    - Maintain all refactorings (e.g., method renaming, modularization, improved readability) present in the Java code.
    - Use only the following Python imports in the translation:
      {allowed_imports_text}
    - Replace any Java library dependencies with equivalent Python libraries where necessary.
    - Handle Python's reserved keywords like `from` or `and` (e.g., `GenerateSequence.from(0)` → `GenerateSequence(start=0)`).
    - Ensure all translated methods are syntactically valid Python code and runnable with the allowed imports.
    - Keep all comments that start with "// Optimized by LLM" and translate them to Python as "# Optimized by LLM" in the corresponding locations.
    - The output must be runnable Python code without any Markdown formatting, explanations, or unnecessary imports.
    - Ensure the translated code preserves the same functionality and structure of the original Java code, with adjustments for Python idioms where applicable.
        
    Java Code:
    {java_code}
    """

    logging.info(f"Sending translation prompt to GPT {prompt}")

    try:
        start_time = time.time()
        response = chatgpt_response_with_backoff(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=TEMPERATURE,
            max_tokens=MAX_LIMIT
        )

        if response.choices:
            python_code = response.choices[0].message.content.strip()
        else:
            python_code = "No translation available."

        end_time = time.time()
        logging.info(f"Translation response time: {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error translating code: {e}")
        return "Error occurred while translating code."

    time.sleep(random.uniform(1, 2))

    clean_code = clean_refactored_code(python_code)
    return clean_code


def clean_imports(code, allowed_imports):
    # Extract all import statements from the translated code
    import_pattern = re.compile(r'^\s*(import .+|from .+ import .+)', re.MULTILINE)
    code_imports = set(import_pattern.findall(code))

    # Identify invalid imports (not in the allowed list)
    invalid_imports = [imp for imp in code_imports if imp not in allowed_imports]

    # Remove invalid imports from the code
    for invalid_import in invalid_imports:
        code = code.replace(invalid_import + "\n", "")

    return code

def clean_refactored_code(refactored_code):
    # Removing the first line that starts with ```python and the last line that is ```
    refactored_code_lines = refactored_code.strip().split('\n')

    if refactored_code_lines and refactored_code_lines[0].startswith("//") and refactored_code_lines[0].endswith("//"):
        refactored_code_lines.pop(0)  # Remove first line
    if refactored_code_lines and refactored_code_lines[-1].strip() == '```':
        refactored_code_lines.pop()  # Remove last line
    if refactored_code_lines and refactored_code_lines[0] == '```python':
        refactored_code_lines.pop(0)  # Remove the first line if it matches '```python'

    # Join back the code
    cleaned_refactored_code = "\n".join(refactored_code_lines)
    return cleaned_refactored_code

def process_files():
    """Process main Java files and their corresponding Python test files."""
    for file_group in SPECIFIC_FILES_TO_TRANSLATE:
        main_file, *test_files = file_group
        process_file_group(main_file, test_files)

def process_file_group(main_file, test_files):
    """Process each main Java file and its test files."""
    main_file_path = find_file_path(main_file, REFACTORED_PATH)

    # Get the corresponding Python file for the main Java file
    main_python_reference = PYTHON_FILE_MAPPING.get(main_file, [None])[0]

    if main_file_path and main_python_reference:
        main_code = read_file(main_file_path)
        save_translated_code(main_python_reference, main_code)
    else:
        logging.warning(f"Could not find main file or mapping for: {main_file}")

    for test_file in test_files:
        # Get the corresponding Python test file
        test_python_reference = PYTHON_FILE_MAPPING.get(main_file, [None, None])[1]
        test_python_path = find_file_path(test_python_reference, PYTHON_PATH)

        if test_python_path and test_python_reference:
            test_code = read_file(test_python_path)
            save_translated_code(test_python_reference, test_code)
        else:
            logging.warning(f"Could not find Python test file for: {test_file}")

def find_file_path(filename, path):
    """Find the full file path given a directory and filename."""
    for root, _, files in os.walk(path):
        if filename in files:
            return os.path.join(root, filename)
    return None

def read_file(path):
    """Read the contents of a file."""
    with open(path, "r", encoding="utf-8") as file:
        return file.read()

def save_translated_code(filename, code):
    """Save code to the output directory using the specified filename."""
    python_file_path = os.path.join(PYTHON_OUTPUT_PATH, filename)
    with open(python_file_path, "w", encoding="utf-8") as python_file:
        python_file.write(code)
    logging.info(f"Saved file: {python_file_path}")

if __name__ == "__main__":
    process_files()
