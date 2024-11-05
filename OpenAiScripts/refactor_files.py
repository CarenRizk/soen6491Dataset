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
PROJECT_PATH = r"C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectJava\src"
OUTPUT_SUGGESTIONS_PATH = os.path.join(PROJECT_PATH, "outputs", "suggestions")
OUTPUT_REFACTORED_PATH = os.path.join(PROJECT_PATH, "outputs", "refactored")

# Create output directories if they don't exist
os.makedirs(OUTPUT_SUGGESTIONS_PATH, exist_ok=True)
os.makedirs(OUTPUT_REFACTORED_PATH, exist_ok=True)

def chatgpt_response_with_backoff(**kwargs):
    client = OpenAI(api_key=api_key)
    return client.chat.completions.create(**kwargs)

def find_refactoring_opportunities(file_path):
    with open(file_path, 'r') as file:
        code = file.read()

    prompt = f"""
    Find all refactoring opportunities in the following code:

    {code}

    Output the suggestions as a numbered list.
    Do not add any styling.
    Only include the suggestions in the output.
    """

    logging.info(f"Sending prompt to GPT for file: {file_path}")

    try:
        start_time = time.time()
        response = chatgpt_response_with_backoff(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=TEMPERATURE,
            max_tokens=MAX_LIMIT
        )

        if response.choices:
            suggestions = response.choices[0].message.content.strip()
        else:
            suggestions = "No suggestions available."

        end_time = time.time()
        logging.info(f"Response time: {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error finding refactoring opportunities: {e}")
        return "Error occurred while generating suggestions."

    time.sleep(random.uniform(1, 2))
    return suggestions

def apply_refactorings(file_path, suggestions):
    with open(file_path, 'r') as file:
        code = file.read()

    logging.info(f"Applying refactorings for file: {file_path}")

    prompt = f"""
    You are a refactoring expert. Refactor the following complete code using all the suggestions provided. 
    
    Make sure you:
    1. For each optimization made, return `// Optimized by LLM: {{Suggestion applied}} //` directly above the code block that was changed.
    2. Return all code Do not remove any methods
    4. Do not add any formatting.
    5. DO NOT modify class names.
    6. DO NOT change the structure of the code.
    7. Never add any additional comments to the code besides that of the instruction in #1.
    
    Suggestions:
    {suggestions}
    
    Code:
    {code}
    """

    try:
        response = chatgpt_response_with_backoff(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=TEMPERATURE,
            max_tokens=MAX_LIMIT
        )

        if response.choices:
            refactored_code = response.choices[0].message.content.strip()
        else:
            refactored_code = code  # No changes if response is empty

    except Exception as e:
        logging.error(f"Error applying refactorings: {e}")
        refactored_code = code  # Fallback to original code

    clean_code = clean_refactored_code(refactored_code)
    return clean_code

def clean_refactored_code(refactored_code):
    # Removing the first line that starts with ```java and the last line that is ```
    refactored_code_lines = refactored_code.strip().split('\n')

    if refactored_code_lines and refactored_code_lines[0].startswith("//") and refactored_code_lines[0].endswith("//"):
        refactored_code_lines.pop(0)  # Remove first line
    if refactored_code_lines and refactored_code_lines[-1].strip() == '```':
        refactored_code_lines.pop()  # Remove last line
    if refactored_code_lines and refactored_code_lines[0] == '```java':
        refactored_code_lines.pop(0)  # Remove the first line if it matches '```java'

    # Join back the code
    cleaned_refactored_code = "\n".join(refactored_code_lines)
    return cleaned_refactored_code

def process_files():
    for root, dirs, files in os.walk(PROJECT_PATH):
        for filename in files:
            if filename.endswith(".java") and filename == "CombineGroupedValues.java":
                file_path = os.path.join(root, filename)
                logging.info(f"Processing file: {file_path}")

                suggestions_file_path = os.path.join(OUTPUT_SUGGESTIONS_PATH, f"{filename}_suggestions.txt")

                if os.path.exists(suggestions_file_path):
                    logging.info(f"Suggestions file already exists: {suggestions_file_path}")

                    # Load suggestions from the file
                    with open(suggestions_file_path, 'r') as suggestions_file:
                        suggestions = suggestions_file.read()
                else:
                    suggestions = find_refactoring_opportunities(file_path)

                    # Save suggestions to a new file
                    suggestions_file_path = os.path.join(OUTPUT_SUGGESTIONS_PATH, f"{filename}_suggestions.txt")
                    with open(suggestions_file_path, 'w') as suggestions_file:
                        suggestions_file.write(suggestions)
                    logging.info(f"Refactoring suggestions saved to: {suggestions_file_path}")

                # Step 2: Apply refactorings
                refactored_code = apply_refactorings(file_path, suggestions)
                refactored_file_path = os.path.join(OUTPUT_REFACTORED_PATH, filename)
                with open(refactored_file_path, 'w') as refactored_file:
                    refactored_file.write(refactored_code)
                logging.info(f"Refactored code saved to: {refactored_file_path}")

if __name__ == "__main__":
    process_files()
