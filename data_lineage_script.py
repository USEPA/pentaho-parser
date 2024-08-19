import os
import pandas as pd
import xml.etree.ElementTree as ET

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
    directory_path = 'etl-code'
    os.makedirs('by_directory_export', exist_ok=True)
    
    # Generate export for each directory
    for root, dirs, _ in os.walk(directory_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]  # Exclude hidden directories
        for subdir in dirs:
            subdir_path = os.path.join(root, subdir)
            print(f"Analyzing directory: {subdir_path}")
            steps, sql_statements, relationships = traverse_directory(subdir_path)
            
            output_file_excel = os.path.join("by_directory_export", f'{subdir}_data_vault_relationships.xlsx')
            output_file_sql = os.path.join("by_directory_export", f'{subdir}_sql_statements.txt')
            
            export_to_excel(steps, sql_statements, relationships, output_file_excel)
            export_sql_statements(sql_statements, output_file_sql)
            
            print(f"Export completed for directory: {subdir_path}. Check the Excel and SQL files for the output.")
    
    # Generate combined export for all directories
    print("Generating combined export...")
    combined_steps, combined_sql_statements, combined_relationships = traverse_directory(directory_path)
    export_to_excel(combined_steps, combined_sql_statements, combined_relationships, 'data_vault_relationships.xlsx')
    export_sql_statements(combined_sql_statements, 'sql_statements.txt')

    print("All directories processed. Combined export completed.")