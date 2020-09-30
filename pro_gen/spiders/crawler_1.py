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
            'ew': 'W',
            'f_st': '15',
            'f_c': '39'
        }, callback=self.start_scraping)

    def start_scraping(self, response):
        all_div = response.css("tr~ tr+ tr a:nth-child(1)").xpath("@href").extract()

        # for a in all_div:
        yield response.follow(all_div[5], callback=self.get_data)

    def get_data(self, response):
        page_data = dict()
        # Add main table
        well_data = response.css('hr+ table tr:nth-child(1) td::text').extract()
        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) td::text").extract()

        if well_data:
            page_data['well_data'] = well_data
        if well_data2:
            page_data['well_data'] = well_data2

        cutting = response.css('table:nth-child(8) td::text').extract()
        if cutting:
            page_data['cutting'] = cutting

        intent = response.css("tr+ tr li a").xpath("@href").extract()
        intent_name = response.css("tr+ tr li a::text").extract()

        if intent:
            page_data['intent'] = intent
        oil_production = response.css('h3+ p a').xpath("@href").extract()

        if oil_production:
            page_data['oil_production_data'] = oil_production

        yield {'elems': page_data}
