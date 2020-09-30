import scrapy
from scrapy.http import FormRequest
# from ..items import TutorialItem
from ..constants import *

class Crawler(scrapy.Spider):
    name = CRAWLER_NAME
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Qualified/index.html'
    ]

    def parse(self, response):
        return FormRequest.from_response(response, formdata={
            'ew' : 'W',
            'f_st': '15',
            'f_c': '3'
        }, callback = self.start_scraping)

    
    def start_scraping(self, response):
        # items = TutorialItem()

        all_div = response.css('tr~ tr+ tr a:nth-child(1)').xpath("@href").extract()
        
        yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1041231775', callback=self.get_page_data)

    
    def get_page_data(self, response):
        pagedata = dict()
        # Add main table
        table_elem = response.css('table:nth-child(5) tr:nth-child(1) td::text').extract()
        if table_elem:
            pagedata['maintable'] = table_elem

        h3tags = response.css('h3::text').extract()
        atags = response.css('h3+ p a').xpath("@href").extract()
        # if 'Oil Production Data' in h3tags:
        #     pagedata['oil_production_data'] = h3tags
            # pagedata['oil_production_data'] = oil_production_data
        if atags:
            pagedata['oil_production_data'] = atags

        yield {'elems' : pagedata}