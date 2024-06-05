import os
import pandas as pd
import re

# Get the current directory
current_directory = os.getcwd()

# List of folders to exclude
excluded_folders = ["city_files", "States", "Arizona"]

# Initialize an empty DataFrame to store the combined data
new_data_df = pd.DataFrame()

# Path to the uszips1.xlsx file
uszips_file_path = 'uszips1.xlsx'

# Load the uszips1 DataFrame once
uszips1_df = pd.read_excel(uszips_file_path, engine='openpyxl')

# Loop through all folders in the current directory
for folder in os.listdir(current_directory):
    if os.path.isdir(folder) and folder not in excluded_folders:
        print(f"Processing data in folder: {folder}")

        # Construct the full input folder path
        input_folder = os.path.join(current_directory, folder)

        # Construct the full cleaned folder path
        cleaned_folder = os.path.join(current_directory, "city_files", folder)

        # Create the cleaned folder if it doesn't exist
        os.makedirs(cleaned_folder, exist_ok=True)

        # Initialize these variables outside the loop
        state_name = ""
        county_name = ""
        city = ""

        # Loop through all files in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv'):
                # Construct the full input file path
                input_file_path = os.path.join(input_folder, filename)

                # Construct the full cleaned file path
                cleaned_file_path = os.path.join(cleaned_folder, filename.lstrip('_').replace('_', ' '))
                file_name, file_extension = os.path.splitext(os.path.basename(cleaned_file_path))

                # Use Pandas to read the CSV file
                df = pd.read_csv(input_file_path)

                def process_location(row):
                    # nonlocal state_name, county_name, city  # Use nonlocal instead of global
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
                        return row  # Return the row
                    else:
                        return None

                # Apply the function to process ZIP codes and filter out rows with invalid ZIP codes
                df = df.apply(process_location, axis=1).dropna(subset=['State', 'City', 'County'])

                # Remove duplicate rows based on the 'Title' column
                df.drop_duplicates(subset='Title', keep='first', inplace=True)

                # Save the cleaned DataFrame to the cleaned folder with the modified name
                try:
                    df.to_csv(cleaned_file_path, index=False)
                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")

                # Iterate through the DataFrame
                for index, row in df.iterrows():
                    title = row.get('Title', '')
                    link = row.get('Link', '')
                    price = row.get('Price', '')
                    date_time = row.get('Date Time', '')
                    location = row.get('Location', '')
                    organiser = row.get('Organiser', '')
                    website_link = row.get('Website Link', '')
                    facebook_link = row.get('Facebook Link', '')
                    twitter_link = row.get('Twitter Link', '')
                    instagram_link = row.get('Instagram Link', '')
                    CITY = row.get('City', '')
                    STATE = row.get('State', '')
                    COUNTY = row.get('County', '')

                    # Prepare the data as a dictionary
                    data_dict = {
                        'State': STATE,
                        'County': COUNTY,
                        'City': CITY,
                        'Region': "",
                        'Neighborhood': "",
                        'Population': "",
                        'Submission Date': "",
                        'Date of Festival': date_time,
                        'Name': title,
                        "Category": "",
                        "Description": "",
                        "Phone": "",
                        "Email Address": "",
                        'Full Address': location,
                        'Website': website_link,
                        "Booking Email": "",
                        "Booking": "",
                        "Submission Link": "",
                        "Booker's Name": organiser,
                        "Type": "",
                        "Genre": "",
                        "Ticketed": "",
                        "Rated": "",
                        'Facebook': facebook_link,
                        'Instagram': instagram_link,
                        'Twitter': twitter_link,
                        "Tiktok": "",
                        "Images": "",
                        "Place ID": ""
                    }

                    # Append the data to the new_data_df
                    new_data_df = new_data_df.append(data_dict, ignore_index=True)

# Remove duplicate rows based on the 'Name' column in the new_data_df DataFrame
new_data_df.drop_duplicates(subset='Name', keep='first', inplace=True)

# Output directory for the combined file
output_template_file_path = 'final_output.csv'

try:
    new_data_df.to_csv(output_template_file_path, index=False)
    print("Data appended to the output file.")
except Exception as e:
    print(f"Error saving the combined data: {str(e)}")
