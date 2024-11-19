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

def extract_imports(directory_path):
    imports = set()
    import_pattern = re.compile(r'^\s*(import .+|from .+ import .+)')

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        match = import_pattern.match(line)
                        if match:
                            imports.add(match.group(0).strip())

    return sorted(imports)

def translate_to_python(java_code, allowed_imports, python_reference):
    allowed_imports_text = "\n".join(allowed_imports)
    prompt = f"""
    Translate the following Java code into Python using the Apache Beam Python SDK.
    - Maintain all refactorings (e.g., method renaming, modularization, improved readability) present in the Java code.
    - Use the provided Python examples as style inspiration while translating the structure and logic directly from the Java code.
    - Replace any Java library dependencies with equivalent Python libraries where necessary.
    - Use only the following Python imports in the translation:
      {allowed_imports_text}
    - Strictly avoid using imports or libraries not listed above.
    - Handle Python's reserved keywords like `from` (e.g., `GenerateSequence.from(0)` → `GenerateSequence(start=0)`).
    - Ensure all translated methods are syntactically valid Python code and runnable with the allowed imports.
    - Retain all comments that start with "// Optimized by LLM" and translate them to Python as "# Optimized by LLM" in the corresponding locations.
    - The output must be runnable Python code without any Markdown formatting, explanations, or unnecessary imports.
    - Ensure the translated code preserves the same functionality and structure of the original Java code, with adjustments for Python idioms where applicable.

    Here are some Python code examples for inspiration:
    {python_reference}
    
    Java Code:
    {java_code}
    """

    logging.info("Sending translation prompt to GPT")

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
    allowed_imports = extract_imports(PYTHON_PATH)

    for file_group in SPECIFIC_FILES_TO_TRANSLATE:
        main_file, *test_files = file_group
        process_file_group(main_file, test_files, allowed_imports)

def process_file_group(main_file, test_files, allowed_imports):
    # Find the paths for the main Java file and its test files
    main_file_path = find_file_path(main_file, REFACTORED_PATH)
    test_file_paths = [find_file_path(test_file, REFACTORED_PATH) for test_file in test_files]

    # Get the main Python file as a reference
    main_python_reference = PYTHON_FILE_MAPPING.get(main_file, [None])[0]
    main_python_code = (
        read_file(find_file_path(main_python_reference, PYTHON_PATH))
        if main_python_reference and find_file_path(main_python_reference, PYTHON_PATH)
        else ""
    )

    if main_file_path:
        # Process main Java file
        main_code = read_file(main_file_path)
        translated_main_code = translate_to_python(main_code, allowed_imports, main_python_code)
        cleaned_main_code = clean_imports(translated_main_code, allowed_imports)
        save_translated_code(main_file, cleaned_main_code)
    else:
        logging.warning(f"Could not find main file: {main_file}")

    for test_file in test_files:
        # Get the corresponding Python test file as a reference
        test_python_reference = PYTHON_FILE_MAPPING.get(main_file, [None, None])[1]
        test_python_code = (
            read_file(find_file_path(test_python_reference, PYTHON_PATH))
            if test_python_reference and find_file_path(test_python_reference, PYTHON_PATH)
            else ""
        )

        test_file_path = find_file_path(test_file, REFACTORED_PATH)
        if test_file_path:
            # Process Java test file
            test_code = read_file(test_file_path)
            translated_test_code = translate_to_python(test_code, allowed_imports, test_python_code)
            cleaned_test_code = clean_imports(translated_test_code, allowed_imports)
            save_translated_code(test_file, cleaned_test_code)
        else:
            logging.warning(f"Could not find test file: {test_file}")

def find_file_path(filename, path):
    for root, _, files in os.walk(path):
        if filename in files:
            return os.path.join(root, filename)
    return None

def read_file(path):
    with open(path, 'r') as file:
        return file.read()

def save_translated_code(filename, code):
    python_filename = filename.replace(".java", ".py")
    python_file_path = os.path.join(PYTHON_OUTPUT_PATH, python_filename)
    with open(python_file_path, 'w') as python_file:
        python_file.write(code)
    logging.info(f"Translated code saved to: {python_file_path}")


if __name__ == "__main__":
    process_files()
