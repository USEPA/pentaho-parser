# Data Vault Relationship Extractor

## Overview

This script extracts SQL queries, business logic, and relationships from Kettle transformation (.ktr) and job (.kjb) files, then organizes the data into both Excel and text file formats. Additionally, it provides a summary of business logic detected across files.

### Key Features:
1. **Extract SQL Queries**: Captures SQL queries from transformation steps in `.ktr` and `.kjb` files.
2. **Business Logic Extraction**: Detects and extracts business logic patterns such as `CASE`, `SUM`, `COUNT`, `GROUP BY`, etc., from SQL queries.
3. **Relationships and Steps Export**: Relationships between tables, schemas, and columns are inferred from the transformation steps and exported to an Excel file, alongside the ETL steps.
4. **SQL Export**: Extracted SQL queries are saved into individual text files and combined into a master file.
5. **Business Logic Summary**: A summary file detailing the business logic types detected in each SQL query is generated, including a master text file with business logic queries.

### Structure of Outputs:

- **SQL Extracts**: SQL queries are saved to text files for each `.ktr` or `.kjb` file. A master SQL extract file is also generated.
- **Business Logic Extracts**: If business logic is detected, the relevant queries are saved in text files alongside the SQL extracts. A master business logic extract file is generated.
- **Excel Export**: Excel files containing relationships, ETL steps, and SQL queries are exported per directory and combined for all directories.
- **Business Logic Summary**: A summary text file with a breakdown of the business logic types (e.g., `CASE`, `GROUP BY`, `SUM`) detected across files.

### Requirements:

- Python 3.x
- `pandas`
- `openpyxl`
- `xml.etree.ElementTree`
- `re` (regex module)

### Usage

1. Update `source_directory` in the script to point to the folder containing your `.ktr` and `.kjb` files.
2. Run the script using Python. The script will recursively scan directories, extract relevant information, and output the results to the directory where the script is located.

```bash
python data_vault_script.py

# File Structure
/source_directory/: Directory containing .ktr and .kjb files.
/by_directory_export/: Output directory containing all extracts and reports.
<subdir>_data_vault_relationships.xlsx: Excel file with relationships and ETL steps.
<subdir>_sql_statements.txt: SQL query extracts.
<subdir>_business_logic.txt: Business logic query extracts (if applicable).
master_sql_extract.txt: Master file with all SQL queries.
master_business_logic_extract.txt: Master file with all business logic queries.
business_logic_summary.txt: Summary of business logic types detected across files.

# Example
The script will create the following structure in the output directory:

/output_directory/
    /by_directory_export/
        /<subdir>/
            <subdir>_data_vault_relationships.xlsx
            <subdir>_sql_statements.txt
            <subdir>_business_logic.txt (if applicable)
    master_sql_extract.txt
    master_business_logic_extract.txt
    business_logic_summary.txt

# Notes
- Ensure your .ktr and .kjb files are properly formatted to allow accurate extraction of SQL and relationships.
- The business logic detection is based on common SQL patterns. Adjust the regular expression in the script to fit specific needs.