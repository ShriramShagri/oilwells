import scrapy
from scrapy.http import FormRequest
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
            'f_c': '205',
            'f_pg': '3'
        }, callback=self.start_scraping)

    def start_scraping(self, response):
        all_div = response.css("tr~ tr+ tr a:nth-child(1)").xpath("@href").extract()

        # for a in all_div:
        yield response.follow(all_div[7], callback=self.get_data)

    def get_data(self, response):
        tabl = response.css("hr+ table a").xpath("@href").extract()
        tabls = response.css("hr+ table a::text").extract()

        print(tabls)

        if tabls[0] == "View Engineering Data" and tabl:
            yield response.follow(tabl[0], callback=self.get_tables)
        else:
            page_data = dict()

            well_data = response.css('hr+ table tr:nth-child(1) td::text').extract()

            well_data2 = response.css("table:nth-child(5) tr:nth-child(1) td::text").extract()

            if len(well_data) > 50:
                page_data['well_data'] = well_data
            elif len(well_data2) > 50:
                page_data['well_data'] = well_data2

            initial_potential = response.css('table:nth-child(7) td+ td::text').extract()

            if initial_potential:
                page_data['IP']: initial_potential

            casing_data = response.css("table:nth-child(9) th , table:nth-child(9) tr+ tr td::text").extract()

            if casing_data:
                page_data['casing'] = casing_data

            perforation_data = response.css("table:nth-child(13) tr+ tr td , table:nth-child(13) th::text").extract()

            if perforation_data:
                page_data['PF'] = perforation_data

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

    def get_tables(self, response):
        page_data = dict()
        # Add main table
        well_data = response.css('hr+ table tr:nth-child(1) td::text').extract()
        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) td::text").extract()
        if len(well_data) > 50:
            page_data['well_data'] = well_data
        elif len(well_data2) > 50:
            page_data['well_data'] = well_data2

        initial_potential = response.css('table:nth-child(7) td+ td::text').extract()

        if initial_potential:
            page_data['IP']: initial_potential

        casing_data = response.css("table:nth-child(9) th::text , table:nth-child(9) tr+ tr td::text").extract()

        if casing_data:
            page_data['casing'] = casing_data

        perforation_data = response.css("table:nth-child(13) td::text , table:nth-child(13) th::text").extract()

        if perforation_data:
            page_data['PF'] = perforation_data

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
