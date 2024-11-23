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
    "Pipeline.java",
    "Trigger.java",
    "Window.java",
    "ApproximateQuantiles.java",
    "ApproximateUnique.java",
    "Combine.java",
    "TextIO.java",
    "Coder.java",
    "Schema.java",
    "PubsubIO.java",
    "FileIO.java",
    "DataflowRunner.java",
    "DirectRunner.java",
    "FileSystems.java",
    "BigtableIO.java",
    "SpannerIO.java",
    "Flatten.java"
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
        Translate the following Java code into Python using the Apache Beam Python SDK. Follow these guidelines strictly:
        
        1. **Strict and Literal Translation**: Perform a one-to-one mapping of the Java code into Python. Do not optimize, refactor, simplify, or alter the functionality, structure, or logic in any way.
        
        2. **Use of Allowed Imports**: Use only the provided Python imports for the translation:
           {allowed_imports_text}
           If a specific Java import has no direct Python equivalent, leave a placeholder with a comment (e.g., `# Placeholder for <Java import>`).
        
        3. **Comments**:
           - Retain all comments from the Java code, translating them into Python as-is.
           - For any comments starting with "// Optimized by LLM" in the Java code, translate them into Python as "# Optimized by LLM" at the corresponding location.
           - Do not add any new comments, explanations, or justifications not present in the original Java code.
        
        4. **Python Syntax Handling**:
           - Handle Python's reserved keywords and syntax differences (e.g., `GenerateSequence.from(0)` → `GenerateSequence(start=0)`).
           - Replace Java-specific constructs or features with their equivalent Python constructs while maintaining strict functionality.
        
        5. **Output Format**:
           - Ensure the output is runnable Python code, compatible with Python 3.8+.
           - Do not include any Markdown formatting, explanations, or extraneous text (e.g., no "```python" or "```").
           - The translated code must be self-contained and syntactically valid.
        
        6. **Preserve Functionality**: The translated code must preserve the same behavior and functionality as the original Java code, adjusting only for Python-specific idioms where strictly necessary.
        
        7. Replace Java-specific placeholders, such as `PCollection`, with equivalent Python constructs. For example:
           - Use `typing.Any` as a generic placeholder type.
           - Use `typing.TypeVar` or `typing.Generic` for generics like `<T>`.
        ---
        
        **Java Code**:
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
    for file in SPECIFIC_FILES_TO_TRANSLATE:
        process_file_group(file)

def process_file_group(main_file):
    """Process each main Java file and its test files."""
    main_file_path = find_file_path(main_file, REFACTORED_PATH)
    if not main_file_path:
        logging.warning(f"Main file not found: {main_file}")
        return

    # Extract the corresponding Python file name
    main_python_reference_name = PYTHON_FILE_MAPPING.get(main_file, [None])[0]
    if not main_python_reference_name:
        logging.warning(f"No Python file mapping found for: {main_file}")
        return

    # Extract allowed imports from the reference file
    main_python_path = find_file_path(main_python_reference_name, PYTHON_PATH)
    allowed_imports = extract_imports_from_file(main_python_path) if main_python_path else []

    # Read Java code
    main_code = read_file(main_file_path)

    # Read Python reference code
    main_python_reference = read_file(main_python_path) if main_python_path else ""

    # Translate Java code to Python
    translated_main = translate_to_python(main_code, allowed_imports, main_python_reference)

    # Save the translated code using the mapped Python filename
    save_translated_code(main_python_reference_name, translated_main)

def save_translated_code(filename, code):
    """Save code to the output directory using the specified filename."""
    python_file_path = os.path.join(PYTHON_OUTPUT_PATH, filename)
    os.makedirs(os.path.dirname(python_file_path), exist_ok=True)  # Ensure the directory exists
    with open(python_file_path, "w", encoding="utf-8") as python_file:
        python_file.write(code)
    logging.info(f"Saved file: {python_file_path}")

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

if __name__ == "__main__":
    process_files()
