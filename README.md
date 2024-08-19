# Data Vault Relationship Extraction

This script traverses a specified directory to parse Kettle (.ktr and .kjb) files, extract ETL steps, infer relationships, and export the results to Excel and SQL files. It generates individual reports for each subdirectory and a combined report for all directories.

## Prerequisites

- Python 3.x
- pandas
- openpyxl
- xml.etree.ElementTree

## Installation

1. Clone the repository or download the script.
2. Ensure you have the required Python packages installed. You can install them using pip:

    ```bash
    pip install pandas openpyxl
    ```

## Usage

1. Place your Kettle files (.ktr and .kjb) and SQL files in a directory.
2. Update the `directory_path` variable in the script to point to your directory.
3. Run the script:

    ```bash
    python data_lineage_script.py
    ```

## Script Overview

The script performs the following steps:

1. **Parse Transformation Steps**: Parses Kettle files to extract ETL steps and SQL statements.
2. **Traverse Directories**: Walks through the specified directory, processing each file and extracting relevant information.
3. **Infer Relationships**: Infers relationships between ETL steps based on common tables and schemas.
4. **Export to Excel and SQL**: Exports the extracted steps, relationships, and SQL statements to Excel and SQL files.

## Outputs

The script generates the following outputs:

1. **By Directory Export**: For each subdirectory, it generates:
    - An Excel file (`<subdir>_data_vault_relationships.xlsx`) containing:
        - `ETL Steps`: Details of each ETL step.
        - `Relationships`: Inferred relationships between steps.
        - `SQL Statements`: Extracted SQL statements.
    - A text file (`<subdir>_sql_statements.txt`) containing all extracted SQL statements.

2. **Combined Export**: For all directories combined, it generates:
    - An Excel file (`data_vault_relationships.xlsx`) containing:
        - `ETL Steps`: Details of each ETL step.
        - `Relationships`: Inferred relationships between steps.
        - `SQL Statements`: Extracted SQL statements.
    - A text file (`sql_statements.txt`) containing all extracted SQL statements.

## Example Output Structure

output/
├── by_directory_export/
│ ├── subdir1_data_vault_relationships.xlsx
│ ├── subdir1_sql_statements.txt
│ ├── subdir2_data_vault_relationships.xlsx
│ ├── subdir2_sql_statements.txt
│ └── ...
├── data_vault_relationships.xlsx
└── sql_statements.txt


## Notes

- Ensure that your directory structure does not contain hidden files or directories unless they are intended to be processed.
- The script handles encoding issues gracefully by skipping files that cannot be read due to encoding problems.