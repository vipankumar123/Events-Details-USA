import os
import pandas as pd

# Get the current directory
current_directory = os.getcwd()

# List all files and directories in the current directory
contents = os.listdir(current_directory)

# List of folders to exclude
excluded_folders = ["city_files", "States"]

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame()
new_data_df = pd.DataFrame()

# Path to the uszips1.xlsx file
uszips_file_path = 'uszips1.xlsx'

# Loop through all folders in the current directory
for folder in contents:
    if os.path.isdir(folder) and folder not in excluded_folders:
        print(f"Processing data in folder: {folder}")

        # Construct the full input folder path
        input_folder = os.path.join(current_directory, folder)

        # Construct the full cleaned folder path
        cleaned_folder = os.path.join(current_directory, "city_files", folder)

        # Create the cleaned folder if it doesn't exist
        os.makedirs(cleaned_folder, exist_ok=True)

        # Output directory for the combined file
        output_folder = 'states'
        output_file_path = os.path.join(output_folder, (input_folder+".csv"))
        output_template_file_path = 'final_output.csv'

        # Create the output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # Path to the uszips1.xlsx file
        uszips_file_path = 'uszips1.xlsx'

        # Loop through all files in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv'):
                # Construct the full input file path
                input_file_path = os.path.join(input_folder, filename)

                # Construct the full cleaned file path
                cleaned_file_path = os.path.join(cleaned_folder, filename.lstrip('_').replace('_', ' '))
                # Use os.path.splitext to split it into name and extension
                file_name, file_extension = os.path.splitext(os.path.basename(cleaned_file_path))

                xlsx_file_path = 'uszips1.xlsx'

                # Use Pandas to read the Excel file
                uszips1_df = pd.read_excel(xlsx_file_path, engine='openpyxl')
                # Check if file_name is present in 'city' column
                state_name = ""
                county_name = ""
                city = ""

                if file_name in uszips1_df['city'].values:
                    # Get the corresponding row
                    matching_row = uszips1_df[uszips1_df['city'] == file_name]

                    state_name = matching_row['state_name'].values[0]
                    county_name = matching_row['county_name'].values[0]
                    city = file_name

                # Load the CSV file into a DataFrame
                df = pd.read_csv(input_file_path)

                # Remove duplicate rows based on the 'Title' column
                df.drop_duplicates(subset='Title', keep='first', inplace=True)

                # Save the cleaned DataFrame to the cleaned folder with the modified name
                try:
                    df.to_csv(cleaned_file_path, index=False)
                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")

                # Select the columns you want to print
                selected_columns = ['Title', 'Link', 'Price', 'Date Time', 'Location', 'Organiser', 'Website Link', 'Facebook Link', 'Twitter Link', 'Instagram Link']
                print(file_name)

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

                    # Prepare the data as a dictionary
                    data_dict = {
                        'State': state_name,
                        'County': county_name,
                        'City': city,
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

                    # Create a new DataFrame with the data
                    new_data_df = new_data_df.append(data_dict, ignore_index=True)

                # Append the data to the combined DataFrame
                combined_df = combined_df.append(df, ignore_index=True)

    # Remove duplicate rows based on the 'Title' column in the combined DataFrame
    combined_df.drop_duplicates(subset='Title', keep='first', inplace=True)
    new_data_df.drop_duplicates(subset='Name', keep='first', inplace=True)

    # Save the combined data to the output file
    try:
        new_data_df.to_csv(output_template_file_path, index=False)
        print("Data appended to the output file.")
    except Exception as e:
        print(f"Error saving the combined data: {str(e)}")

# Save the combined DataFrame to the output file
try:
    combined_df.to_csv(output_file_path, index=False)
    print("Combined CSV file saved as 'California.csv' in the 'states' folder.")
except Exception as e:
    print(f"Error saving the combined file: {str(e)}")