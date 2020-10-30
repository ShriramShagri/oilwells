import scrapy
from scrapy.http import FormRequest, Request
from ..items import ProGenItem
from ..constants import *
import os
from dateutil.parser import parse

page = 1
index = 0


class Crawler(scrapy.Spider):
    name = CRAWLER_NAME
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Qualified/index.html'
    ]

    # REPLACE self.start_scrapping BY parse METHOD!!!
    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        global page

        #  Fill the main form Below
        page = 1
        return FormRequest.from_response(response, formdata={
            'ew': 'W',
            'f_st': '15',
            'f_c': str(COUNTY[0]),
            'f_pg': str(page)
        }, callback=self.start_scraping)

    def start_scraping(self, response):
        '''
        This function is used to collect all links in the column per page and iterate through all of them
        '''
        global page, index
        wellColumn = response.css("tr~ tr+ tr a:nth-child(1)::attr(href)").extract()

        # Itetrate Through All links per page
        self.logger.info('No. of links: %s', len(wellColumn))
        if len(wellColumn) > 3:
            for a in wellColumn:
                if len(a)<80:
                    yield response.follow(a,
                                callback=self.get_data)
            page += 1
            yield response.follow(
                    f"https://chasm.kgs.ku.edu/ords/qualified.ogw5.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={COUNTY[index]}&f_ws=ALL&f_api=&sort_by=&f_pg={page}",
                callback=self.start_scraping)
        elif index < len(COUNTY) - 1:
            index += 1
            page = 1
            yield response.follow(
                    f"https://chasm.kgs.ku.edu/ords/qualified.ogw5.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={COUNTY[index]}&f_ws=ALL&f_api=&sort_by=&f_pg={page}",
                callback=self.start_scraping)


    def get_data(self, response):

        '''
        This function is used to Data from general page
        '''
        self.items = ProGenItem()
        # Use KID of current link for file and folder names

        # global CURRENTKID, CURRENTAPI
        CURRENTKID, CURRENTAPI = '', ''
        # Collect WH table and Send to Pipeline

        well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()

        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()

        # Simple check for valid data
        if 'API: ' in well_data or 'KID: ' in well_data:
            self.items['wh'] = well_data
            # Set KID for current link
            CURRENTAPI = well_data[well_data.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data[well_data.index('KID: ') + 1].replace("\n", "")
        elif 'API: ' in well_data2 or 'KID: ' in well_data2:
            self.items['wh'] = well_data2
            # Set KID for current link
            CURRENTAPI = well_data2[well_data2.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data2[well_data2.index('KID: ') + 1].replace("\n", "")

        # Get All H3 tags to recognise all tables in the page

        headers = response.css('h3::text').extract()

        # check if cuttings tabale is present, if present, Send to pipeline

        if "Cuttings Data" in headers:
            cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
            if cutting:
                if 3 < len(cutting) < 15:
                    self.items['cutting'] = cutting
            else:
                cutting = response.css(
                    f'table:nth-child({headers.index("Cuttings Data") * 2}) ::text').extract()  # Care to be taken if multiple css selectors are present
                if cutting:
                    if 3 < len(cutting) < 15:
                        self.items['cutting'] = cutting

        # Check for all pdfs present and download. To manage all the pdfs that get downloaded, goto constants.py file

        if "ACO-1 and Driller's Logs" in headers:
            tempdownloadlist = list(TODOWNLOAD)
            pdfherf = response.css('li a::attr(href)').extract()
            pdf = response.css('li a::text').extract()

            drillreportherf = response.css('tr:nth-child(3) a::attr(href)').extract()
            drillreport = response.css('tr:nth-child(3) a::text').extract()
            if 'Digital Wellbore information for this horizontal well is available.' in drillreport:
                tempdownloadlist.remove('Directional Drilling Report')
                url = drillreportherf[
                    drillreport.index('Digital Wellbore information for this horizontal well is available.')]
                yield Request(
                    url=response.urljoin(url),
                    callback=self.save_file,
                    meta={
                        'filename': 'Directional Drilling Report' + CURRENTKID + CURRENTAPI + "." + url.split('.')[-1],
                        'kid': CURRENTKID, 'api': CURRENTAPI}
                )

            count = 0
            for i in range(len(pdf)):
                if pdf[i] in tempdownloadlist:
                    count += 1
                    yield Request(
                        url=response.urljoin(pdfherf[i]),
                        callback=self.save_file,
                        meta={
                            'filename': pdf[i] + str(count) + CURRENTKID + CURRENTAPI + "." + pdfherf[i].split('.')[-1],
                            'kid': CURRENTKID, 'api': CURRENTAPI}
                    )

        # Check for oil Production page. If so redirect and initiate .txt file Download

        if "Oil Production Data" in headers:
            oil_production = response.css('h3+ p a').xpath("@href").extract()

            if oil_production:
                yield response.follow(
                    oil_production.pop().replace('.MainLease?', '.MonthSave?'),
                    callback=self.getOilData,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})  # replace mainlease to skip a page

        # check if Tops tabale is present, if present save to database

        if "Tops Data" in headers:
            tops = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()

            if (len(tops) <= 3 or "Because there are more than 30 tops for this well, " in tops) and tops:
                # Redirect to tops table page

                topspage = response.css(
                    f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) a::attr(href)').extract()
                yield response.follow(
                    topspage[-1],
                    callback=self.gettopspage,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})

            else:
                # Save tops table data

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()
                self.topsSegregation(topspage, CURRENTKID, CURRENTAPI)

        # If DST Scans Available...
        if "DST Data" in headers:
            dst = response.css('td td a::text').extract()
            dstlink = response.css('td td a').xpath("@href").extract()

            # print(dst, dstlink)

            if "Scans" in dst and "Available" in dst:
                yield response.follow(
                    dstlink[dst.index("Scans")],
                    callback=self.getScans,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})

        # Check if Engineering Data Page is present

        toggleEngineering = response.css("hr+ table a").xpath("@href").extract()
        toggleEngineeringText = response.css("hr+ table a::text").extract()
        self.items['api'], self.items['kid'] = CURRENTAPI, CURRENTKID

        # Change function if Engineering Data Page is found
        yield self.items

        if toggleEngineeringText:
            if toggleEngineeringText[0] == "View Engineering Data" and toggleEngineering:
                yield response.follow(toggleEngineering[0], callback=self.get_tables,
                                      meta={'api': CURRENTAPI, 'kid': CURRENTKID})

    def get_tables(self, response):
        '''
        This function is used to Data from engineering page
        '''
        self.items = ProGenItem()
        # global CURRENTKID, CURRENTAPI
        CURRENTKID, CURRENTAPI = '', ''
        CURRENTKID, kid = response.meta.get('kid'), response.meta.get('kid')
        CURRENTAPI, api = response.meta.get('api'), response.meta.get('api')
        self.logger.info('Switched to Engineering Data: KID= %s', CURRENTKID)

        self.items['api'], self.items['kid'] = api, kid
        # Get All H3 tags to recognise all headings inside tables in the page

        headers = response.css('td h3::text').extract()

        if "Casing record" in headers:
            # Collect casing table and Send to Pipeline (Multiple css elector path might be present)

            casing_data = response.css("table:nth-child(9) ::text").extract()

            self.items['casing'] = casing_data

        if "Perforation Record" in headers:
            # Collect pf table and Send to Pipeline (Multiple css elector path might be present)

            perforationHeaders = response.css('table:nth-child(13) th').extract()
            perforation_data = response.css("table:nth-child(13) tr+ tr td").extract()

            self.items['pfHeaders'] = perforationHeaders
            self.items['pf'] = perforation_data

        # Collect IP table and Send to Pipeline (Multiple css elector path might be present)

        initial_potential = response.css('table:nth-child(7) td ::text').extract()

        if initial_potential:
            self.items['ip'] = initial_potential

        # Get All H3 tags to recognise all tables in the page

        headers = response.css('h3::text').extract()

        # check if cuttings tabale is present, if present, Send to pipeline

        if "Cuttings Data" in headers:
            self.logger.info('Cuttings Data present: KID= %s', CURRENTKID)
            cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
            if cutting:
                if 3 < len(cutting) < 15:
                    self.items['cutting'] = cutting
            else:
                cutting = response.css(f'table:nth-child({headers.index("Cuttings Data") * 2}) ::text').extract()
                if cutting:
                    if 3 < len(cutting) < 15:
                        self.items['cutting'] = cutting

        # Check for all pdfs present and download. To manage all the pdfs that get downloaded, goto constants.py file

        if "ACO-1 and Driller's Logs" in headers:
            cuttinghref = response.css('li a::attr(href)').extract()
            cutting = response.css('li a::text').extract()

            count = 0
            for i in range(len(cutting)):
                if cutting[i] in TODOWNLOAD:
                    count += 1
                    yield Request(
                        url=response.urljoin(cuttinghref[i]),
                        callback=self.save_file,
                        meta={'filename': cutting[i] + str(count) + CURRENTKID + CURRENTAPI + cuttinghref[i].split('.')[
                            -1], 'kid': CURRENTKID, 'api': CURRENTAPI}
                    )

        # Check for oil Production page. If so redirect and initiate .txt file Download

        if "Oil Production Data" in headers:
            self.logger.info('Oil Production Data present: KID= %s', CURRENTKID)
            oil_production = response.css('h3+ p a').xpath("@href").extract()

            if oil_production:
                yield response.follow(
                    oil_production.pop().replace('.MainLease?', '.MonthSave?'),
                    callback=self.getOilData,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})

        if "Tops Data" in headers:
            self.logger.info('Tops Data present: KID= %s', CURRENTKID)
            tops = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()

            if (len(tops) <= 3 or "Because there are more than 30 tops for this well, " in tops) and tops:
                # Redirect to tops table page

                topspage = response.css(
                    f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) a::attr(href)').extract()
                yield response.follow(
                    topspage[-1],
                    callback=self.gettopspage,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})

            else:
                # Save tops table data

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()
                self.topsSegregation(topspage, CURRENTKID, CURRENTAPI)

        # Check for DST scans...
        if "DST Data" in headers:
            dst = response.css('td td a::text').extract()
            dstlink = response.css('td td a').xpath("@href").extract()

            if "Scans" in dst and "Available" in dst:
                yield response.follow(
                    dstlink[dst.index("Scans")],
                    callback=self.getScans,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI})

        yield self.items

    def getScans(self, response):
        kid = response.meta.get('kid')
        api = response.meta.get('api')

        links = response.css('a::attr(href)').extract()

        for i in links:
            if i.split('.')[-1] in DST_Extensions:
                yield Request(
                    url=response.urljoin(i),
                    callback=self.save_file,
                    meta={'filename': "DSTReport" + kid + api + "." + i.split('.')[-1], 'kid': kid, 'api': api}
                )

    def gettopspage(self, response):
        '''
        This function collects tops data from redirected page
        '''
        # Collect the table
        kid = response.meta.get('kid')
        api = response.meta.get('api')
        fulltable = response.css('tr+ tr td::text').extract()
        self.logger.info('Tops data in new page: KID= %s', kid)
        self.topsSegregation(fulltable[2:], kid, api)

    def topsSegregation(self, data, kid, api):
        '''
        This function stores Tops table data into database
        '''

        def checkDate(i):
            try:
                try:
                    k = int(i)
                    return False
                except:
                    pass
                parse(i)
                return True
            except ValueError:
                return False

        # Remove and segregate data

        topsRawData = [i.replace('\n', "").replace('\xa0', '') for i in data]
        temp = list()
        topsFilteredData = []
        for item in topsRawData:
            temp.append(item)
            if checkDate(item) or len(temp) == 6:
                if len(temp) == 5:
                    temp.insert(2, '')
                elif len(temp) == 4:
                    temp.insert(1, '')
                    temp.insert(2, '')
                if len(temp) == 6:
                    topsFilteredData.append([api, kid] + [temp[0] + temp[1]] + temp[2:])
                temp = []

        # Push to database
        try:
            self.logger.info('Storing Tops data: KID= %s', kid)
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in topsFilteredData).decode(
                "utf-8")
            DATABASE.cur.execute("INSERT INTO tops VALUES " + args_str)
        except Exception as e:
            self.logger.info('Tops data failed: KID= %s', kid)
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "Tops"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

    def getOilData(self, response):
        '''
        This function is used to redirect and initiate file download for Oil Production Data
        '''
        # get KID from metadata

        ckid = response.meta.get('kid')
        capi = response.meta.get('api')
        proceed = False

        #  Get txt file link and download

        oil_data_href = response.css('a').xpath('@href').extract()
        for item in oil_data_href:
            if item.split('.').pop() == 'txt':
                proceed = item
        if proceed:
            yield Request(
                url=response.urljoin(oil_data_href[-1]),
                callback=self.save_oil_data,
                meta={'filename': f"oil_production_{ckid}_{capi}.txt", 'kid': ckid, 'api': capi}
            )

    def save_file(self, response):
        '''
        This function is used for downloading files
        '''
        # Get filename from metadata

        filename = response.meta.get('filename')
        kid = response.meta.get('kid')
        api = response.meta.get('api')

        # Setup appropriate path and create directory
        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[index]))):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[index])))
        if len(api) > 4:
            pass
        else:
            api = 'NOAPI'

        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api)):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api))

        path = os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api, filename)

        # Save the file

        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)

    def save_oil_data(self, response):
        '''
        This function reads oil production txt file and saves it first
        then writs the content fron text file to csv and saves the contents in postgres
        '''
        # Collect filename and KID from metadata

        filename = response.meta.get('filename')
        kid = response.meta.get('kid')
        api = response.meta.get('api')

        if len(api) > 4:
            pass
        else:
            api = 'NOAPI'

        # Setup appropriate path and create directory

        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[index]))):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[index])))

        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api)):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api))

        path = os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api, filename)
        dirpath = os.path.join(STORAGE_PATH, str(COUNTY[index]), kid + '_' + api)

        self.logger.info('Saving txt %s', path)

        # Write the text file

        with open(path, 'wb') as file:
            file.write(response.body)

        # read downloaded text file and convert to postgres ready text file
        lines = []
        with open(path, 'r') as in_file:
            stripped = [line.strip().strip('"') for line in in_file]

            for l in stripped[1:-1]:  # Ignore header
                lines.append(api + ';' + kid + ';' + l.replace('","', ';') + '\n')
            lines.append(api + ';' + kid + ';' + stripped[-1].replace('","', ';'))

        with open(path, 'w') as out_file:
            out_file.writelines(lines)

        # Save to database

        try:
            with open(path, 'r') as f:
                # next(f) # Skip the header row.
                DATABASE.cur.copy_from(f, 'oilProduction', sep=';')
        except Exception as e:
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "oilproduction"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()
