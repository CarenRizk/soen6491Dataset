import os
import logging
import time
import random
from openai import OpenAI

# Configure OpenAI API
api_key = ''
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
TEMPERATURE = 0.2
TOKEN_LIMIT = 12000
MAX_LIMIT = 16100
MODEL = "gpt-4o-mini"
REFACTORED_PATH = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectJavaRefactored"
PYTHON_OUTPUT_PATH = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectPythonTranslated"
SPECIFIC_FILES_TO_TRANSLATE = [
    "WindowingTest.java",
    "SpannerIOWriteTest.java",
    "SpannerIOReadTest.java",
    "SpannerIOReadChangeStreamTest.java",
    "TextIOReadTest.java",
    "TextIOWriteTest.java"
]

# Create output directory if it doesn't exist
os.makedirs(PYTHON_OUTPUT_PATH, exist_ok=True)

def chatgpt_response_with_backoff(**kwargs):
    client = OpenAI(api_key=api_key)
    return client.chat.completions.create(**kwargs)

def translate_to_python(java_code):
    prompt = f"""
    Translate the following Java code into Python. Make sure all translated methods are complete and keep the structure as close to the original as possible.
    
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
    return python_code

def process_files():
    for root, dirs, files in os.walk(REFACTORED_PATH):
        for filename in files:
            if filename in SPECIFIC_FILES_TO_TRANSLATE and filename.endswith(".java"):
                file_path = os.path.join(root, filename)
                logging.info(f"Processing file: {file_path}")

                # Read the refactored Java code
                with open(file_path, 'r') as file:
                    java_code = file.read()

                # Translate to Python
                python_code = translate_to_python(java_code)

                # Save the translated Python code in the specified directory
                python_filename = filename.replace(".java", ".py")
                python_file_path = os.path.join(PYTHON_OUTPUT_PATH, python_filename)
                with open(python_file_path, 'w') as python_file:
                    python_file.write(python_code)

                logging.info(f"Translated code saved to: {python_file_path}")

if __name__ == "__main__":
    process_files()
