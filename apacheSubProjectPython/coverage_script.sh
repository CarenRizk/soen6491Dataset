#!/bin/bash

# Activate virtual environment (if you have one)
# python -m venv venv
# venv\Scripts\activate
# pip install -r requirements.txt\\ or C:\Users\carenrizk\repos\soen6491Dataset\apacheSubProjectPython\venv\Scripts\python.exe -m pip install -r requirements.txt

echo "Running tests with coverage..."

# Run all tests in the tests directory with coverage
coverage  run -m pytest

echo "Generating coverage report..."

# Generate the coverage report
coverage report
# Generate HTML coverage report (optional)
coverage html

start htmlcov/index.html

echo "Coverage report generated."

# Deactivate virtual environment (if you activated it)
# deactivate
