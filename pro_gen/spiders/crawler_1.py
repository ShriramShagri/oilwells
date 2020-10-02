import scrapy
from scrapy.http import FormRequest
from ..items import ProGenItem
from ..constants import *
import os
from scrapy.http import Request

CURRENTKID = ""


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
        self.items = ProGenItem()
        all_div = response.css("tr~ tr+ tr a:nth-child(1)").xpath("@href").extract()

        # for a in all_div:
        yield response.follow(all_div[5],
                              callback=self.get_data)

    def get_data(self, response):
        global CURRENTKID
        tabl = response.css("hr+ table a").xpath("@href").extract()
        tabls = response.css("hr+ table a::text").extract()

        print(tabls)

        if tabls[0] == "View Engineering Data" and tabl:
            yield response.follow(tabl[0], callback=self.get_tables)
        else:
            page_data = dict()

            well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()

            well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()

            if len(well_data) > 50:
                self.items['wh'] = well_data
                CURRENTKID = well_data[5].replace("\n", "")
            elif len(well_data2) > 50:
                self.items['wh'] = well_data2
                CURRENTKID = well_data2[5].replace("\n", "")

            initial_potential = response.css('table:nth-child(7) td ::text').extract()

            if initial_potential:
                self.items['ip'] = initial_potential

            # casing_data = response.css("table:nth-child(9) th , table:nth-child(9) tr+ tr td::text").extract()

            # if casing_data:
            #     self.items['casing'] = casing_data
            #
            # perforation_data = response.css("table:nth-child(13)::text").extract()
            #
            # if perforation_data:
            #     self.items['pf'] = perforation_data

            headers = response.css('h3::text').extract()
            if "Cuttings Data" in headers:
                cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
                if cutting:
                    self.items['cutting'] = cutting

            intent = response.css("li:nth-child(1) a").xpath("@href").extract()
            intent_name = response.css("li:nth-child(1) a::text").extract()
            for name in intent_name:
                if name == "Intent To Drill Well":
                    temp = intent[intent_name.index("Intent To Drill Well")]
                    yield Request(
                        url=response.urljoin(temp),
                        callback=self.save_pdf
                    )

            print(intent)
            # for href in response.css("li:nth-child(1) a").xpath("@href").extract():
            #     yield Request(
            #         url=response.urljoin(href),
            #         callback=self.save_pdf
            #     )
            if intent:
                page_data['intent'] = intent

            oil_production = response.css('h3+ p a').xpath("@href").extract()
            if oil_production:
                page_data['oil_production_data'] = oil_production

            yield self.items

    def get_tables(self, response):
        global CURRENTKID
        page_data = dict()
        # Add main table
        well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()
        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()
        if len(well_data) > 50:
            self.items['wh'] = well_data
            CURRENTKID = well_data[5].replace("\n", "")
        elif len(well_data2) > 50:
            self.items['wh'] = well_data2
            CURRENTKID = well_data[5].replace("\n", "")

        initial_potential = response.css('table:nth-child(7) td ::text').extract()

        if initial_potential:
            self.items['ip'] = initial_potential

        casing_data = response.css("table:nth-child(9) ::text").extract()

        if casing_data:
            self.items['casing'] = casing_data

        perforation_data = response.css("table:nth-child(13) ::text").extract()

        if perforation_data:
            self.items['pf'] = perforation_data

        cutting = response.css('table:nth-child(8) ::text').extract()
        if cutting:
            self.items['cutting'] = cutting

        intent = response.css("li:nth-child(1) a").xpath("@href").extract()
        intent_name = response.css("li:nth-child(1) a::text").extract()
        print(intent)

        for name in intent_name:
            if name == "Intent To Drill Well" :
                temp = intent[intent_name.index("Intent To Drill Well")]
                yield Request(
                    url=response.urljoin(temp),
                    callback=self.save_pdf
                )

        # for href in response.css("li:nth-child(1) a").xpath("@href").extract():
        #     yield Request(
        #         url=response.urljoin(intent[intent_name.index("Intent To Drill Well")]),
        #         callback=self.save_pdf
        #     )
        if intent:
            page_data['intent'] = intent

        oil_production = response.css('h3+ p a').xpath("@href").extract()
        if oil_production:
            page_data['oil_production_data'] = oil_production

        yield self.items

    def save_pdf(self, response):
        cwd = os.getcwd()
        if not os.path.isdir(os.path.join(cwd, "docs", CURRENTKID)):
            os.mkdir(os.path.join(cwd, "docs", CURRENTKID))
        path = os.path.join(cwd, "docs", CURRENTKID, f"{CURRENTKID}.pdf")
        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)

