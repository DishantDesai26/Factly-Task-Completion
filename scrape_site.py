import scrapy # type: ignore
from scrapy.crawler import CrawlerProcess # type: ignore
import os
import re

class NCRBDataSpider(scrapy.Spider):
    name = 'ncrb_data_spider'
    base_url = 'https://ncrb.gov.in/accidental-deaths-suicides-in-india-table-content.html?year={year}&category='

    def start_requests(self):
        """Start by fetching the first page to extract available years."""
        yield scrapy.Request(self.base_url, callback=self.parse_years)

    def parse_years(self, response):
        """Extract available years from the dropdown menu."""
        years = response.xpath('//select[@name="year"]/option/@value').getall()
        
        if not years:
            self.logger.warning("No years found! Check the XPath or website structure.")
            return
        
        for year in years:
            year_url = self.base_url.format(year=year)
            yield scrapy.Request(year_url, callback=self.parse_year_data, meta={'year': year})

    def parse_year_data(self, response):
        year = response.meta['year']  # Get the year dynamically

        #  Search for the PDF in the page
        regex = rf".*Incidence and Rate of Suicides\s+(?!.*to\b).*"
        pdf_link_element = response.xpath(
        f'//a[re:test(text(), "{regex}")]'
        )

        if pdf_link_element:
            pdf_name = pdf_link_element.xpath('./text()').get()
            pdf_link = pdf_link_element.xpath('./@href').get()

            if pdf_link:
                absolute_pdf_link = response.urljoin(pdf_link)
                yield {
                    'year': year,
                    'pdf_url': absolute_pdf_link,
                    'file_name': f'{pdf_name}_{year}.pdf'
                }
            else:
                self.logger.warning(f'PDF link not found for {pdf_name} in year {year}')
        else:
            self.logger.warning(f'PDF for year {year} not found in expected formats.')

# --- PDF Download Pipeline ---
from scrapy.pipelines.files import FilesPipeline # type: ignore

def sanitize_filename(filename):
        return re.sub(r'[\\/*?:"<>|]', '_', filename)  # Replace invalid characters with `_`

class PDFDownloaderPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        """Requests the PDF file."""
        return scrapy.Request(item['pdf_url'], meta={'file_name': item['file_name']})

    def file_path(self, request, response=None, info=None, item=None):
        """Saves the file in the 'pdfs' directory with a meaningful name."""
        file_name = sanitize_filename(request.meta["file_name"])
        return f'pdfs/{file_name}'

# --- Run the Scraper ---
if __name__ == '__main__':
    if not os.path.exists('pdfs'):
        os.makedirs('pdfs')

    process = CrawlerProcess({
        'DOWNLOAD_DELAY': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'ITEM_PIPELINES': {
            '__main__.PDFDownloaderPipeline': 1,
        },
        'FILES_STORE': '.',
    })

    process.crawl(NCRBDataSpider)
    process.start()