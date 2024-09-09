import os
import pandas as pd
import xml.etree.ElementTree as ET
import re

def log(message):
    print(message)

# Parse transformation steps from Kettle files
def parse_trans_steps(root):
    steps = []
    sql_statements = []
    for step in root.findall(".//step"):
        step_name = step.find('name').text if step.find('name') is not None else ''
        step_type = step.find('type').text if step.find('type') is not None else ''
        step_schema = step.find('.//schema').text if step.find('.//schema') is not None else ''
        step_table = step.find('.//table').text if step.find('.//table') is not None else ''
        step_fields = [field.find('name').text for field in step.findall('.//field') if field.find('name') is not None]
        sql_content = step.find('.//sql').text if step.find('.//sql') is not None else ''
        
        steps.append({
            'file': root.attrib.get('filename', ''),
            'step_name': step_name,
            'step_type': step_type,
            'schema': step_schema,
            'table': step_table,
            'fields': step_fields
        })
        
        if sql_content:
            sql_statements.append({
                'file': root.attrib.get('filename', ''),
                'step_name': step_name,
                'sql': sql_content
            })
    
    return steps, sql_statements

# Parse Kettle files (.ktr and .kjb)
def parse_kettle_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    return parse_trans_steps(root)

# Traverse directories and parse Kettle files
def traverse_directory(directory_path):
    steps = []
    sql_statements = []
    relationships = []

    for root_dir, sub_dirs, files in os.walk(directory_path):
        sub_dirs[:] = [d for d in sub_dirs if not d.startswith('.')]  # Exclude hidden directories
        
        for file_name in files:
            if file_name.startswith('.'):
                continue  # Exclude hidden files
            file_path = os.path.join(root_dir, file_name)
            if file_name.endswith(('.ktr', '.kjb')):
                try:
                    file_steps, file_sql_statements = parse_kettle_file(file_path)
                    for step in file_steps:
                        step['file'] = file_path  # Add the file path to each step
                    steps.extend(file_steps)
                    sql_statements.extend(file_sql_statements)
                except Exception as e:
                    log(f"Error processing file {file_path}: {e}")
            elif file_name.endswith('.sql'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as sql_file:
                        sql_content = sql_file.read()
                        sql_statements.append({
                            'file': file_path,
                            'sql': sql_content
                        })
                except UnicodeDecodeError:
                    log(f"Skipping file due to encoding issue: {file_path}")
                except Exception as e:
                    log(f"Error reading SQL file {file_path}: {e}")

    relationships = infer_relationships(steps)  # Generate relationships
    return steps, sql_statements, relationships

# Infer relationships between steps based on common tables and schemas
def infer_relationships(steps):
    relationships = []
    table_map = {}

    for step in steps:
        table_key = (step['schema'], step['table'])
        if table_key not in table_map:
            table_map[table_key] = {'input': [], 'output': []}
        if step['step_type'] in ['TableInput', 'DBLookup']:
            table_map[table_key]['input'].append(step)
        elif step['step_type'] in ['TableOutput', 'InsertUpdate', 'PGBulkLoader']:
            table_map[table_key]['output'].append(step)

    for table_key, step_dict in table_map.items():
        inputs = step_dict['input']
        outputs = step_dict['output']
        for inp in inputs:
            for out in outputs:
                relationships.append({
                    'source_file': inp['file'],
                    'source_step': inp['step_name'],
                    'source_type': inp['step_type'],
                    'source_schema': inp['schema'],
                    'source_table': inp['table'],
                    'source_columns': inp['fields'],
                    'target_file': out['file'],
                    'target_step': out['step_name'],
                    'target_type': out['step_type'],
                    'target_schema': out['schema'],
                    'target_table': out['table'],
                    'target_columns': out['fields'],
                })
    return relationships

# Export steps, relationships, and SQL statements to Excel
def export_to_excel(steps, sql_statements, relationships, output_file):
    df_steps = pd.DataFrame(steps)
    if 'fields' in df_steps.columns:
        df_steps = df_steps.explode('fields')  # Ensure each field is in a separate row

    df_sql = pd.DataFrame(sql_statements)

    df_relationships = pd.DataFrame(relationships)
    if 'source_columns' in df_relationships.columns and 'target_columns' in df_relationships.columns:
        df_relationships = df_relationships.explode('source_columns').explode('target_columns')

    with pd.ExcelWriter(output_file) as writer:
        if not df_steps.empty:
            df_steps.to_excel(writer, sheet_name='ETL Steps', index=False)
        if not df_sql.empty:
            df_sql.to_excel(writer, sheet_name='SQL Statements', index=False)
        if not df_relationships.empty:
            df_relationships.to_excel(writer, sheet_name='Relationships', index=False)
        else:
            # Ensure at least one sheet is created to avoid the IndexError
            pd.DataFrame({'Message': ['No relationships found']}).to_excel(writer, sheet_name='NoData', index=False)

def extract_sql_from_files(source_directory, output_directory):
    # Adjusted regular expression to more strictly match SQL queries.
    sql_pattern = re.compile(r'<sql>([\s\S]*?)<\/sql>', re.IGNORECASE)

    # Expanded pattern to detect all forms of embedded business logic
    business_logic_pattern = re.compile(
        r'\b(CASE\b|SUM\(|AVG\(|COUNT\(|MIN\(|MAX\(|ROW_NUMBER\(|RANK\(|LEAD\(|LAG\(|'
        r'IF\(|COALESCE\(|NULLIF\(|ISNULL\(|DATEDIFF\(|DATEADD\(|CONCAT\(|'
        r'CAST\(|CONVERT\(|TO_CHAR\(|TO_DATE\(|GROUP BY\b|HAVING\b|ARRAY_AGG\(|'
        r'UNNEST\(|JSON_OBJECT\(|JSON_ARRAYAGG\(|JSON_EXTRACT\(|STRING_AGG\(|'
        r'ABS\(|ROUND\(|CEIL\(|FLOOR\(|UPPER\(|LOWER\(|TRIM\(|REPLACE\()',
        re.IGNORECASE
    )

    combined_sql_queries = []  # To store all SQL queries for the master SQL extract
    combined_business_logic_queries = []  # To store all business logic queries for the master business logic extract
    summary_data = []  # To store summary information for each file

    # Walk through the directory structure of the source
    for root, dirs, files in os.walk(source_directory):
        for file in files:
            if file.endswith(".ktr") or file.endswith(".kjb"):
                file_path = os.path.join(root, file)

                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Capture SQL queries using stricter pattern
                sql_matches = sql_pattern.findall(content)
                logic_summary = {}

                if sql_matches:
                    # Prepare output directories based on script's working directory
                    relative_path = os.path.relpath(root, source_directory)  # Mirror structure
                    output_dir = os.path.join(output_directory, relative_path)
                    os.makedirs(output_dir, exist_ok=True)

                    logic_summary['file'] = file_path
                    logic_summary['sql_query_count'] = len(sql_matches)
                    logic_summary['business_logic_types'] = set()

                    # Create directory-level SQL extract
                    output_filename = os.path.join(output_dir, f"{file}_extract.txt")
                    with open(output_filename, 'w') as output_file:
                        output_file.write(f"File Count: 1\n")
                        output_file.write(f"File Name: {file_path}\n\n")
                        
                        for query in sql_matches:
                            query_clean = query.strip()
                            output_file.write(f"SQL Query:\n{query_clean}\n\n")

                            # Add to master SQL extract
                            combined_sql_queries.append({
                                'file': file_path,
                                'query': query_clean
                            })

                            # Check for business logic and update summary
                            if business_logic_pattern.search(query_clean):
                                business_logic_filename = os.path.join(output_dir, f"{file}_business_logic.txt")
                                with open(business_logic_filename, 'a') as business_logic_file:
                                    if os.path.getsize(business_logic_filename) == 0:
                                        business_logic_file.write(f"File Count: 1\n")
                                        business_logic_file.write(f"File Name: {file_path}\n\n")
                                    
                                    business_logic_file.write(f"Business Logic Query:\n{query_clean}\n\n")

                                combined_business_logic_queries.append({
                                    'file': file_path,
                                    'query': query_clean
                                })

                                # Detect specific logic types
                                for logic_type in ['CASE', 'SUM', 'COUNT', 'GROUP BY', 'HAVING']:
                                    if logic_type in query_clean:
                                        logic_summary['business_logic_types'].add(logic_type)

                    summary_data.append(logic_summary)

    # Write the summary data to a separate file
    summary_filename = os.path.join(output_directory, "business_logic_summary.txt")
    with open(summary_filename, 'w') as summary_file:
        summary_file.write("Summary of Business Logic Across Files\n")
        summary_file.write("="*50 + "\n\n")
        for entry in summary_data:
            summary_file.write(f"File: {entry['file']}\n")
            summary_file.write(f"Total SQL Queries: {entry['sql_query_count']}\n")
            summary_file.write(f"Business Logic Types Detected: {', '.join(entry['business_logic_types']) if entry['business_logic_types'] else 'None'}\n")
            summary_file.write("\n" + "="*50 + "\n\n")

    # Write the combined SQL queries to a master file with summary stats
    combined_sql_filename = os.path.join(output_directory, "master_sql_extract.txt")
    with open(combined_sql_filename, 'w') as master_sql_file:
        master_sql_file.write(f"File Count: {len(set(entry['file'] for entry in combined_sql_queries))}\n")
        master_sql_file.write(f"File Names: {', '.join(set(entry['file'] for entry in combined_sql_queries))}\n\n")
        
        for entry in combined_sql_queries:
            master_sql_file.write(f"File: {entry['file']}\n")
            master_sql_file.write(f"SQL Query:\n{entry['query']}\n")
            master_sql_file.write("\n" + "="*80 + "\n\n")

    # Write the combined business logic queries to a master business logic file
    if combined_business_logic_queries:
        combined_business_logic_filename = os.path.join(output_directory, "master_business_logic_extract.txt")
        with open(combined_business_logic_filename, 'w') as master_business_logic_file:
            master_business_logic_file.write(f"File Count: {len(set(entry['file'] for entry in combined_business_logic_queries))}\n")
            master_business_logic_file.write(f"File Names: {', '.join(set(entry['file'] for entry in combined_business_logic_queries))}\n\n")
            
            for entry in combined_business_logic_queries:
                master_business_logic_file.write(f"File: {entry['file']}\n")
                master_business_logic_file.write(f"Business Logic Query:\n{entry['query']}\n")
                master_business_logic_file.write("\n" + "="*80 + "\n\n")


# Export SQL statements to a text file
def export_sql_statements(sql_statements, output_file):
    with open(output_file, 'w') as file:
        for sql_statement in sql_statements:
            file.write(f"File: {sql_statement['file']}\n")
            file.write(f"Step: {sql_statement.get('step_name', 'N/A')}\n")
            file.write(f"SQL:\n{sql_statement['sql']}\n")
            file.write("\n" + "="*80 + "\n\n")

# Main function
if __name__ == "__main__":
    source_directory = '../etl-code'  # Directory with .ktr and .kjb files
    output_directory = os.path.dirname(os.path.realpath(__file__))  # Directory where the script is located
    os.makedirs(output_directory, exist_ok=True)
    
    # Generate export for each directory
    for root, dirs, _ in os.walk(source_directory):
        dirs[:] = [d for d in dirs if not d.startswith('.')]  # Exclude hidden directories
        for subdir in dirs:
            subdir_path = os.path.join(root, subdir)
            print(f"Analyzing directory: {subdir_path}")
            steps, sql_statements, relationships = traverse_directory(subdir_path)
            
            # Store in output_directory, maintaining the sub-directory structure
            relative_subdir = os.path.relpath(subdir_path, source_directory)
            output_subdir = os.path.join(output_directory, relative_subdir)
            os.makedirs(output_subdir, exist_ok=True)
            
            output_file_excel = os.path.join(output_subdir, f'{subdir}_data_vault_relationships.xlsx')
            output_file_sql = os.path.join(output_subdir, f'{subdir}_sql_statements.txt')
            
            export_to_excel(steps, sql_statements, relationships, output_file_excel)
            export_sql_statements(sql_statements, output_file_sql)
            
            print(f"Export completed for directory: {subdir_path}. Check the Excel and SQL files for the output.")
    
    # Run the SQL and business logic extraction
    print("Extracting SQL and Business Logic...")
    extract_sql_from_files(source_directory, output_directory)
    
    # Generate combined export for all directories
    print("Generating combined export...")
    combined_steps, combined_sql_statements, combined_relationships = traverse_directory(source_directory)
    export_to_excel(combined_steps, combined_sql_statements, combined_relationships, os.path.join(output_directory, 'data_vault_relationships.xlsx'))
    export_sql_statements(combined_sql_statements, os.path.join(output_directory, 'sql_statements.txt'))

    print("All directories processed. Combined export completed.")