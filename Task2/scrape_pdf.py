import PyPDF2 # type: ignore
import pandas as pd # type: ignore
import re
import os

# Folder containing PDFs
pdf_folder = os.path.join(os.getcwd(), "Task2/pdfs")

# Folder to store extracted Excel files
sheets_folder = os.path.join(os.getcwd(), "Task2/sheets")
os.makedirs(sheets_folder, exist_ok=True)  # Ensure the output folder exists

def extract_table_data(text, section):
    """Extracts tabular data under the given section header."""
    section_index = text.find(section)
    if section_index == -1:
        return []

    section_text = text[section_index:].split("TOTAL")[0]  # Extract only relevant part

    # Regular expression to extract structured table data
    pattern = re.compile(r"(\d+)\s+([A-Z&()'\- ]+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)")
    matches = pattern.findall(section_text)

    return [list(row) for row in matches]

# Find all PDFs in the folder
pdf_files = [f for f in os.listdir(pdf_folder)]

for pdf_file in pdf_files:
    pdf_path = os.path.join(pdf_folder, pdf_file)
    print(f"Processing: {pdf_file}")

    # Extract text using PyPDF2
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = "\n".join([page.extract_text() or "" for page in reader.pages])  # Handle None values

    # Extract tables separately for States and UTs
    states_data = extract_table_data(text, "STATES:")
    uts_data = extract_table_data(text, "UNION TERRITORIES:")

    # Convert to DataFrames
    columns = ["Sl. No.", "State/UT", "Number of Suicides", "Percentage Share", "Estimated Mid-Year Population (Lakh)", "Rate of Suicides"]
    
    states_df = pd.DataFrame(states_data, columns=columns)
    uts_df = pd.DataFrame(uts_data, columns=columns)

    # Create an Excel file inside the "sheets" folder
    output_file = os.path.join(sheets_folder, f"{os.path.splitext(pdf_file)[0]}.xlsx")

    with pd.ExcelWriter(output_file) as writer:
        states_df.to_excel(writer, sheet_name="States", index=False)
        uts_df.to_excel(writer, sheet_name="Union Territories", index=False)

    print(f"Data successfully saved to {output_file}")