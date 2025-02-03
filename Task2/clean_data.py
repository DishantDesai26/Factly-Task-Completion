import os
import pandas as pd # type: ignore
import re

# Define the folder where Excel files are stored
base_folder = os.path.join(os.getcwd(), "Task2")

# Define input (sheets) and output (processed) folder paths
excel_folder = os.path.join(base_folder, "sheets")  # Contains input Excel files
processed_folder = os.path.join(base_folder, "processed")  # Folder for the output file

# Ensure the processed folder exists
os.makedirs(processed_folder, exist_ok=True)

# List all Excel files in the folder
excel_files = [f for f in os.listdir(excel_folder)]

# Initialize an empty list to store reformatted data from all files
all_data = []

for excel_file in excel_files:
    file_path = os.path.join(excel_folder, excel_file)
    print(f"Processing file: {excel_file}")

    # Extract the 4-digit year from the filename using regex
    match = re.search(r"\b(19|20)\d{2}\b", excel_file)
    year = int(match.group()) if match else None  # Assign None if no year is found

    try:
        # Read only the "States" sheet from the Excel file (ignoring Union Territories)
        states_df = pd.read_excel(file_path, sheet_name="States", engine="openpyxl")

        # Rename columns to ensure consistency in structure
        states_df.columns = ["Sl. No.", "state", "Number of Suicides", "Percentage Share in Total",
                             "Projected Mid Year Population", "Rate of Suicides"]

        # Convert relevant columns to numeric values, handling errors gracefully
        numeric_cols = ["Number of Suicides", "Percentage Share in Total", 
                        "Projected Mid Year Population", "Rate of Suicides"]
        
        for col in numeric_cols:
            states_df[col] = pd.to_numeric(states_df[col], errors="coerce")
        
        # Convert state names to Title Case
        states_df["state"] = states_df["state"].astype(str).str.title()

        # Add the extracted year as a new column
        states_df["year"] = year

        # Restructure data so that each state has four corresponding records (one per category)
        reformatted_data = []
        
        for _, row in states_df.iterrows():
            state = row["state"]
            
            # Define categories and corresponding units
            categories = [
                ("Number of Suicides", "Value in Absolute number"),
                ("Percentage Share in Total", "Value in Percentage"),
                ("Projected Mid Year Population", "Value in Lakh"),
                ("Rate of Suicides", "Value in Ratio")
            ]

            # Create separate rows for each category
            for category, unit in categories:
                reformatted_data.append({
                    "year": row["year"],
                    "state": state,
                    "category": category,
                    "value": row[category],
                    "unit": unit,
                    "note": None
                })

        # Convert the reformatted list into a DataFrame
        structured_df = pd.DataFrame(reformatted_data)

        # Append this file's processed data to the main list
        all_data.append(structured_df)

    except Exception as e:
        print(f"Error processing {excel_file}: {e}")

# If data was extracted from at least one file, proceed to create the final Excel output
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)

    # Sort data by year and state to ensure logical ordering
    final_df = final_df.sort_values(by=["year", "state"])

    # Define the output file path
    output_file = os.path.join(processed_folder, "NCRB Cleaned Data.xlsx")

    # Save the final DataFrame to an Excel file with the default sheet name
    final_df.to_excel(output_file, index=False)

    print(f"Consolidated data successfully saved to {output_file}")
else:
    print("No valid data found to consolidate.")