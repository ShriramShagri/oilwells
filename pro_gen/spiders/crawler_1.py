import scrapy
from scrapy.http import FormRequest
from ..items import ProGenItem
from ..constants import *
import os
import csv
from scrapy.http import Request

CURRENTKID = ""


class Crawler(scrapy.Spider):
    name = CRAWLER_NAME
    # Start Url
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
        yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1002880481',
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
            print(headers)
            if "Cuttings Data" in headers:
                cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
                if cutting:
                    if 3 < len(cutting) < 15:
                        self.items['cutting'] = cutting
                else:
                    cutting = response.css(f'table:nth-child({headers.index("Cuttings Data") * 2}) ::text').extract()
                    if cutting:
                        if 3 < len(cutting) < 15:
                            self.items['cutting'] = cutting


            if "ACO-1 and Driller's Logs" in headers:
                cuttinghref = response.css('b+ ul a::attr(href)').extract()
                cutting = response.css('b+ ul a::text').extract()
                
                print(cutting, cuttinghref)
                # count = 0
                # for i in range(len(cutting)):
                #     if cutting[i] in TODOWNLOAD:
                #         count += 1
                #         yield Request(
                #         url=response.urljoin(cuttinghref[i]),
                #         callback=self.save_file,
                #         meta={'filename' : cutting[i]+str(count)+"."+cuttinghref[i].split('.')[-1]}
                #     )

            if "Oil Production Data" in headers:
                oil_production = response.css('h3+ p a').xpath("@href").extract()
                # print(oil_production)

                if oil_production:
                    yield response.follow(oil_production.pop().replace('.MainLease?', '.MonthSave?'), callback=self.getOilData, meta={'kid': CURRENTKID})
                    

            yield self.items

    def getOilData(self, response):
        ckid = response.meta.get('kid')
        proceed = False

        oil_data_href = response.css('a').xpath('@href').extract()
        for item in oil_data_href:
            if item.split('.').pop() == 'txt':
                proceed = item
        if proceed:
            yield Request(
                    url=response.urljoin(oil_data_href[-1]),
                    callback=self.save_csv,
                    meta={'filename' : "oil_production.txt", 'kid' : ckid}
                    )
            

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

        headers = response.css('h3::text').extract()
        print(headers)
        if "Cuttings Data" in headers:
            cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
            if cutting:
                self.items['cutting'] = cutting
        if "ACO-1 and Driller's Logs" in headers:
            cuttinghref = response.css('b+ ul a::attr(href)').extract()
            cutting = response.css('b+ ul a::text').extract()
            
            print(cutting, cuttinghref)
            count = 0
            for i in range(len(cutting)):
                if cutting[i] in TODOWNLOAD:
                    count += 1
                    yield Request(
                    url=response.urljoin(cuttinghref[i]),
                    callback=self.save_file,
                    meta={'filename' : cutting[i]+str(count)+cuttinghref[i].split('.')[-1]}
                )

        oil_production = response.css('h3+ p a').xpath("@href").extract()
        if oil_production:
            page_data['oil_production_data'] = oil_production

        yield self.items

    def save_file(self, response):
        filename = response.meta.get('filename')
        cwd = os.getcwd()
        if not os.path.isdir(os.path.join(cwd, "docs", CURRENTKID)):
            os.mkdir(os.path.join(cwd, "docs", CURRENTKID))
        path = os.path.join(cwd, "docs", CURRENTKID, filename)
        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)
    
    def save_csv(self, response):
        filename = response.meta.get('filename')
        kid = response.meta.get('kid')
        cwd = os.getcwd()
        if not os.path.isdir(os.path.join(cwd, "docs", CURRENTKID)):
            os.mkdir(os.path.join(cwd, "docs", CURRENTKID))
        path = os.path.join(cwd, "docs", CURRENTKID, filename)
        dirpath = os.path.join(cwd, "docs", CURRENTKID)
        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)
        with open(path, 'r') as in_file:
            stripped = (line.strip().replace('"', '') for line in in_file)
            templines =  [line.split(",") for line in stripped if line]
            lines = []
            # print(lines)
            for l in templines:
                lines.append(tuple([kid]) + tuple(l))
            # print(lines)
            with open(os.path.join(dirpath, filename.split('.')[0] + ".csv"), 'w', newline='') as out_file:
                writer = csv.writer(out_file)
                writer.writerows(lines)
        
        with open(path, 'r') as f:
            next(f) # Skip the header row.
            DATABASE.cur.copy_from(f, 'oilProduction', sep=',')

        DATABASE.conn.commit()
        # with open(path, 'wb') as file:
        #     file.write(response.body)

