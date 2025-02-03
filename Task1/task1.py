import scrapy
import os
import PyPDF2
from io import BytesIO
import pandas as pd  # type: ignore
from scrapy.crawler import CrawlerProcess
import re

class NCRBSpider(scrapy.Spider):
    name = "ncrb_spider"
    start_urls = ["https://ncrb.gov.in/accidental-deaths-suicides-in-india-table-content.html?year=2022&category="]

    def parse(self, response):
        # Find all links to PDFs on the page
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()

        for link in pdf_links:
            pdf_url = response.urljoin(link)
            yield scrapy.Request(pdf_url, callback=self.parse_pdf)

    def parse_pdf(self, response):
        # Get the filename from the URL and change its extension to .xlsx
        filename = response.url.split("/")[-1].replace(".pdf", ".xlsx")

        # Define the folder where extracted data will be saved
        save_path = os.path.join(os.getcwd(), "Task1", "raw")

        try:
            # Make sure the folder exists before saving any files
            os.makedirs(save_path, exist_ok=True)

            # Read the PDF content from the response
            pdf_file = BytesIO(response.body)
            reader = PyPDF2.PdfReader(pdf_file)

            text_content = ""
            for page in reader.pages:
                extracted_text = page.extract_text()
                if extracted_text:
                    text_content += extracted_text + "\n"

            # Convert the extracted text into a structured format
            if text_content.strip():
                cleaned_df = self.clean_text_to_dataframe(text_content)
                if cleaned_df is not None:
                    file_path = os.path.join(save_path, filename)
                    cleaned_df.to_excel(file_path, index=False, header=False)
                    self.log(f"Saved extracted data to: {file_path}")
                else:
                    self.log(f"Error processing text from: {filename}")
        
        except Exception as e:
            self.log(f"Error processing PDF: {e}")

    def clean_text_to_dataframe(self, text):
        """Converts extracted text into a structured DataFrame by splitting columns based on spaces or tabs."""
        try:
            rows = text.split("\n")
            data = [re.split(r'\s{2,}|\t', row.strip()) for row in rows if row.strip()]  # Splitting on multiple spaces or tabs
            return pd.DataFrame(data)
        except Exception as e:
            self.log(f"Error cleaning text: {e}")
            return None

def main():
    process = CrawlerProcess()
    process.crawl(NCRBSpider)
    process.start()

if __name__ == "__main__":
    main()
