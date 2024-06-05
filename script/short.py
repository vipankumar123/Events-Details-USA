import os
import pandas as pd
import re
from concurrent.futures import ProcessPoolExecutor

def process_file(input_file_path, cleaned_file_path, uszips1_df):
    def process_location(row):
        state_name = ""
        county_name = ""
        city = ""
        loc = row['Location']
        zip_code_match = re.search(r'\b\d{5}$', loc)
        if zip_code_match:
            zip_code = zip_code_match.group()
            matching_row = uszips1_df[uszips1_df['zip'] == int(zip_code)]
            if not matching_row.empty:
                state_name = matching_row['state_name'].values[0]
                county_name = matching_row['county_name'].values[0]
                city = matching_row['city'].values[0]
            row['State'] = state_name
            row['County'] = county_name
            row['City'] = city
            return row
        else:
            return None

    df = pd.read_csv(input_file_path)
    df = df.apply(process_location, axis=1).dropna(subset=['State', 'City', 'County'])
    df.drop_duplicates(subset=['State', 'City', 'County', 'Title'], keep='first', inplace=True)

    try:
        df.to_csv(cleaned_file_path, index=False)
    except Exception as e:
        print(f"Error processing: {str(e)}")

    return df

def main():
    current_directory = os.getcwd()
    excluded_folders = ["city_files", "States"]
    uszips_file_path = 'uszips1.xlsx'
    uszips1_df = pd.read_excel(uszips_file_path, engine='openpyxl')

    new_data_df = pd.DataFrame()  # Create an empty DataFrame

    for folder in os.listdir(current_directory):
        if os.path.isdir(folder) and folder not in excluded_folders:
            print(f"Processing data in folder: {folder}")

            input_folder = os.path.join(current_directory, folder)
            cleaned_folder = os.path.join(current_directory, "city_files", folder)
            os.makedirs(cleaned_folder, exist_ok=True)

            futures = []
            for filename in os.listdir(input_folder):
                if filename.endswith('.csv'):
                    input_file_path = os.path.join(input_folder, filename)
                    cleaned_file_path = os.path.join(cleaned_folder, filename.lstrip('_').replace('_', ' '))
                    futures.append((executor.submit(process_file, input_file_path, cleaned_file_path, uszips1_df), filename))

            for future, filename in futures:
                try:
                    df = future.result()
                    new_data_df = pd.concat([new_data_df, df], ignore_index=True)
                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")

    new_data_df.drop_duplicates(subset=['State', 'City', 'County', 'Title'], keep='first', inplace=True)

    output_template_file_path = 'final_output.csv'
    try:
        new_data_df.to_csv(output_template_file_path, index=False)
        print("Data appended to the output file.")
    except Exception as e:
        print(f"Error saving the combined data: {str(e)}")

if __name__ == "__main__":
    with ProcessPoolExecutor() as executor:
        main()
