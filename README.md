# Hotel Data Engineer Project

## Overview

This project is designed to manage hotel reservation data and perform various data transformations. It includes modules for handling data transformations and database operations, as well as a suite of tests to ensure functionality and data integrity.

## Project Structure

- `etl/`: Contains the ETL (Extract, Transform, Load) scripts and transformation functions.
- `tests/`: Contains test scripts for validating the functionality of transformation and database operations.
- `data/`: Directory where sample data files may be stored (if applicable).

## Setup

### Prerequisites

- Python 3.8 or higher
- SQLAlchemy
- Pandas
- Pytest
- SQLite (for local testing)

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/hotel-data-engineer.git
    cd hotel-data-engineer
    ```

2. Create a virtual environment:
    ```bash
    python -m venv venv
    ```

3. Activate the virtual environment:
    - On Windows:
      ```bash
      venv\Scripts\activate
      ```
    - On macOS/Linux:
      ```bash
      source venv/bin/activate
      ```

4. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Running the Transformations

To execute the transformations, run the following script:
```bash
python etl/transformations.py
```

### Running Tests
To run the tests, use the following command:
```bash
pytest
```

## Directory and File Descriptions

- etl/transformations.py: Contains functions for transforming data, including standardizing room types and formatting phone numbers.
- etl/extract_transform_load.py: Contains functions for interacting with the database (not included in this snippet, add as needed).
- tests/test_transformations.py: Tests for transformation functions.
- tests/test_db_operations.py: Tests for database operations (not included in this snippet, add as needed).