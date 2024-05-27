def count():
    import pandas as pd

    filename = '3_sybil_report_data/final/grouped_results.csv'

    df = pd.read_csv(filename).dropna()
    row_count = len(df)

    print(f': {row_count}')


def search():
    import os
    import re
    directory_path = '3_sybil_report_data/result/user'
    matches = set()
    pattern = re.compile(r'The first 11 addresses of the cluster share the ')
    for filename in os.listdir(directory_path):
        if filename.endswith('.txt') is False and filename.endswith('.md') is False and filename.endswith(
                '.csv') is False:
            continue
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='UTF-8') as file:
                for line in file:
                    found_matches = pattern.findall(line)
                    if found_matches:
                        print(found_matches)
                        print(file_path)


def simplify():
    import pandas as pd

    filename = 'address_ok_attributes_done.csv'

    df = pd.read_csv(filename).dropna()
    df.to_csv('address_ok_attributes_done_dropna.csv', index=False)


import os
import pandas as pd


def count_csv_rows(folder_path, output_file):
    # 
    results = []

    # 
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                # CSV
                df = pd.read_csv(file_path)
                row_count = len(df)
                results.append([filename, row_count])
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

    # DataFrame
    result_df = pd.DataFrame(results, columns=['Filename', 'RowCount'])

    # CSV
    result_df.to_csv(output_file, index=False)
    print(f"Result saved to {output_file}")


def generate_md_excel():
    import pandas as pd

    def csv_to_markdown(csv_file_path):
        # CSV
        df = pd.read_csv(csv_file_path)

        # 
        columns = df.columns.tolist()

        # Markdown
        header = '| ' + ' | '.join(columns) + ' |'
        separator = '| ' + ' | '.join(['---'] * len(columns)) + ' |'

        # Markdown
        rows = df.apply(lambda row: '| ' + ' | '.join(map(str, row)) + ' |', axis=1).tolist()

        # 
        markdown_table = '\n'.join([header, separator] + rows)

        return markdown_table

    # 
    csv_file_path = r'3_sybil_report_data\report\REPORT\REPORT\Group2\group2\group__datetime.date_2023,_12,_30_,_datetime.date_2024,_4,_29_,__Fantom_,_4.0_.csv'
    markdown_output = csv_to_markdown(csv_file_path)

    # ，
    output_file_path = 'md_excel.md'
    with open(output_file_path, 'w') as file:
        file.write(markdown_output)

    print(f"Markdown table saved to {output_file_path}")


generate_md_excel()