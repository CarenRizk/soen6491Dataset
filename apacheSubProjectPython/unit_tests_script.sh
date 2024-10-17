#!/bin/bash

# --- run script to run all unit tests
# pytest "$unit_tests_script"

# --- get coverage by unit tests
# coverage run pipeline_test.py

# Activate virtual environment (if any)
# source /path/to/your/virtualenv/bin/activate

# Specify the test files to run
TEST_FILES=(
    "pipeline_test.py"
    "trigger_test.py"
    "window_test.py"
    "approximatequantiles_test.py"
    "approximateunique_test.py"
    "combiners_test.py"
    "textio_test.py"
    "coders_test.py"
    "schemas_test.py"
    "pubsub_test.py"
    "fileio_test.py"
    "dataflow_runner_test.py"
    "direct_runner_test.py"
    "filesystems_test.py"
    "bigtableio_test.py"
    "spannerio_test.py"
    "mongodbio_test.py"
    "flatten_test.py"
)

# Run each specified test file
for test_file in "${TEST_FILES[@]}"
    do
        echo "Running tests in $test_file"
        python -m unittest "$test_file"
    done

# Deactivate virtual environment (if needed)
# deactivate


# pip install apache-beam
# pip install pyyaml
# pip install PyHamcrest
# pip install pandas
# pip install parameterized
# pip install tenacity