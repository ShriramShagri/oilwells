import scrapy
from scrapy.http import FormRequest
from ..items import ProGenItem
from ..constants import *
import os
import csv
from scrapy.http import Request

CURRENTKID = ""
CURRENTAPI = ''


class Crawler(scrapy.Spider):
    name = CRAWLER_NAME
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Qualified/index.html'
    ]

    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        self.items = ProGenItem()

        #  Fill the main form Below
        return FormRequest.from_response(response, formdata={
            'ew': 'W',
            'f_st': '15',
            'f_c': '205',
            'f_pg': '3'
        }, callback=self.start_scraping)

    def start_scraping(self, response):
        '''
        This function is used to collect all links in the column per page and iterate through all of them
        '''

        wellColumn = response.css("tr~ tr+ tr a:nth-child(1)").xpath("@href").extract()

        # Itetrate Through All links per page

        # for a in wellColumn:
        yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1006067479',
                              callback=self.get_data)

    def get_data(self, response):
        '''
        This function is used to Data from general page
        '''
        # Use KID of current link for file and folder names

        global CURRENTKID, CURRENTAPI

        # Collect WH table and Send to Pipeline (Multiple css elector path might be present)

        well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()

        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()

        # Simple check for valid data
        if 'API: ' in well_data:
            self.items['wh'] = well_data
            # Set KID for current link
            CURRENTAPI = well_data[well_data.index('API: ')+1].replace("\n", "")
            CURRENTKID = well_data[well_data.index('KID: ')+1].replace("\n", "")
        elif 'API: ' in well_data2:
            self.items['wh'] = well_data2
            # Set KID for current link
            CURRENTAPI = well_data2[well_data2.index('API: ')+1].replace("\n", "")
            CURRENTKID = well_data2[well_data2.index('KID: ')+1].replace("\n", "")
        
        # Get All H3 tags to recognise all tables in the page

        headers = response.css('h3::text').extract()

        # check if cuttings tabale is present, if present, Send to pipeline

        if "Cuttings Data" in headers:
            cutting = response.css(f'table:nth-child({(headers.index("Cuttings Data") + 1) * 2}) ::text').extract()
            if cutting:
                if 3 < len(cutting) < 15:
                    self.items['cutting'] = cutting
            else:
                cutting = response.css(f'table:nth-child({headers.index("Cuttings Data") * 2}) ::text').extract() # Care to be taken if multiple css selectors are present
                if cutting:
                    if 3 < len(cutting) < 15:
                        self.items['cutting'] = cutting

        # Check for all pdfs present and download. To manage all the pdfs that get downloaded, goto constants.py file

        if "ACO-1 and Driller's Logs" in headers:
            tempdownloadlist = list(TODOWNLOAD)
            pdfherf = response.css('b+ ul a::attr(href)').extract()
            pdf = response.css('b+ ul a::text').extract()

            drillreportherf = response.css('tr:nth-child(3) a::attr(href)').extract()
            drillreport = response.css('tr:nth-child(3) a::text').extract()
            if 'Digital Wellbore information for this horizontal well is available.' in drillreport:
                tempdownloadlist.remove('Directional Drilling Report')
                url = drillreportherf[drillreport.index('Digital Wellbore information for this horizontal well is available.')]
                yield Request(
                    url=response.urljoin(url),
                    callback=self.save_file,
                    meta={'filename' : 'Directional Drilling Report'+"."+url.split('.')[-1]}
                )
            
            count = 0
            for i in range(len(pdf)):
                if pdf[i] in tempdownloadlist:
                    count += 1
                    yield Request(
                    url=response.urljoin(pdfherf[i]),
                    callback=self.save_file,
                    meta={'filename' : pdf[i]+str(count)+"."+pdfherf[i].split('.')[-1]}
                )

        # Check for oil Production page. If so redirect and initiate .txt file Download

        if "Oil Production Data" in headers:
            oil_production = response.css('h3+ p a').xpath("@href").extract()

            if oil_production:
                yield response.follow(
                    oil_production.pop().replace('.MainLease?', '.MonthSave?'), 
                    callback=self.getOilData, 
                    meta={'kid': CURRENTKID, 'api' : CURRENTAPI}) # replace mainlease to skip a page
        
        # check if Tops tabale is present, if present save to database

        if "Tops Data" in headers:
            tops = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()

            if len(tops) <= 5:
                # Redirect to tops table page

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) a::attr(href)').extract()
                yield response.follow(
                    topspage[-1], 
                    callback=self.gettopspage, 
                    meta={'kid': CURRENTKID, 'api' : CURRENTAPI})
            
            else:
                # Save tops table data

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()
                self.topsSegregation(topspage)
                
        # Check if Engineering Data Page is present

        toggleEngineering = response.css("hr+ table a").xpath("@href").extract()
        toggleEngineeringText = response.css("hr+ table a::text").extract()

        # Change function if Engineering Data Page is found

        if toggleEngineeringText[0] == "View Engineering Data" and toggleEngineering:
            yield response.follow(toggleEngineering[0], callback=self.get_tables)

        else:

            # Collect IP table and Send to Pipeline (Multiple css elector path might be present)

            # FATAL: CSS SELECTOR TO BE FIXED!!!

            initial_potential = response.css('table:nth-child(7) td ::text').extract()

            if initial_potential:
                self.items['ip'] = initial_potential

            # Collect Casing table and Send to Pipeline (Multiple css elector path might be present)
            
            # FATAL: CSS SELECTOR TO BE FIXED!!!


            # casing_data = response.css("table:nth-child(9) th , table:nth-child(9) tr+ tr td::text").extract()

            # if casing_data:
            #     self.items['casing'] = casing_data

            # Collect Pf table and Send to Pipeline (Multiple css elector path might be present)
            
            # FATAL: CSS SELECTOR TO BE FIXED!!!

            #
            # perforation_data = response.css("table:nth-child(13)::text").extract()
            #
            # if perforation_data:
            #     self.items['pf'] = perforation_data
                    
            yield self.items

    def get_tables(self, response):
        '''
        This function is used to Data from engineering page
        '''
        # Get All H3 tags to recognise all headings inside tables in the page

        headers = response.css('td h3::text').extract()
        
        if "Casing record" in headers:
            # Collect casing table and Send to Pipeline (Multiple css elector path might be present)

            # FATAL: CSS SELECTOR TO BE FIXED!!!
            casing_data = response.css("table:nth-child(9) ::text").extract()

            self.items['casing'] = casing_data
        
        if "Perforation Record" in headers:
            # Collect pf table and Send to Pipeline (Multiple css elector path might be present)

            # FATAL: CSS SELECTOR TO BE FIXED!!!
            perforation_data = response.css("table:nth-child(13) ::text").extract()

            self.items['pf'] = perforation_data

        # Collect IP table and Send to Pipeline (Multiple css elector path might be present)

        # FATAL: CSS SELECTOR TO BE FIXED!!!

        initial_potential = response.css('table:nth-child(7) td ::text').extract()

        if initial_potential:
            self.items['ip'] = initial_potential
      
        # Get All H3 tags to recognise all tables in the page

        headers = response.css('h3::text').extract()

        # check if cuttings tabale is present, if present, Send to pipeline

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
        
        # Check for all pdfs present and download. To manage all the pdfs that get downloaded, goto constants.py file

        if "ACO-1 and Driller's Logs" in headers:
            cuttinghref = response.css('b+ ul a::attr(href)').extract()
            cutting = response.css('b+ ul a::text').extract()
            
            count = 0
            for i in range(len(cutting)):
                if cutting[i] in TODOWNLOAD:
                    count += 1
                    yield Request(
                    url=response.urljoin(cuttinghref[i]),
                    callback=self.save_file,
                    meta={'filename' : cutting[i]+str(count)+cuttinghref[i].split('.')[-1]}
                )

        # Check for oil Production page. If so redirect and initiate .txt file Download
            
        if "Oil Production Data" in headers:
                oil_production = response.css('h3+ p a').xpath("@href").extract()

                if oil_production:
                    yield response.follow(
                        oil_production.pop().replace('.MainLease?', '.MonthSave?'), 
                        callback=self.getOilData, 
                        meta={'kid': CURRENTKID, 'api' : CURRENTAPI})
        
        if "Tops Data" in headers:
            tops = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()

            if len(tops) <= 5:
                # Redirect to tops table page

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) a::attr(href)').extract()
                yield response.follow(
                    topspage[-1], 
                    callback=self.gettopspage, 
                    meta={'kid': CURRENTKID, 'api' : CURRENTAPI})
            
            else:
                # Save tops table data

                topspage = response.css(f'table:nth-child({(headers.index("Tops Data") + 1) * 2}) td::text').extract()
                self.topsSegregation(topspage)

        yield self.items
    
    def gettopspage(self, response):
        '''
        This function collects tops data from redirected page
        '''
        # Collect the table
        fulltable = response.css('tr+ tr td::text').extract()
        self.topsSegregation(fulltable[len(fulltable)%6:])

    def topsSegregation(self, data):
        '''
        This function stores Tops table data into database
        '''
        # Remove and segregate data

        topsRawData = [i.replace('\n', "").replace('\xa0', '') for i in data]
        topsRawData = [topsRawData[i:i + 6] for i in range(0, len(topsRawData), 6)]

        topsFilteredData= []

        for i in topsRawData:
            temp = list()
            temp.extend([CURRENTAPI,CURRENTKID])
            temp.extend([i[0] + i[1]] + i[2:])
            topsFilteredData.append(temp)
        
        # Push to database

        args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in topsFilteredData).decode("utf-8")
        DATABASE.cur.execute("INSERT INTO tops VALUES " + args_str)
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
                    meta={'filename' : "oil_production.txt", 'kid' : ckid, 'api' : capi}
                    )
            

    def save_file(self, response):
        '''
        This function is used for downloading files
        '''
        # Get filename from metadata

        filename = response.meta.get('filename')

        # Setup appropriate path and create directory

        cwd = os.getcwd()
        if not os.path.isdir(os.path.join(cwd, "docs", CURRENTKID)):
            os.mkdir(os.path.join(cwd, "docs", CURRENTKID))

        path = os.path.join(cwd, "docs", CURRENTKID, filename)

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

        # Setup appropriate path and create directory

        cwd = os.getcwd()
        if not os.path.isdir(os.path.join(cwd, "docs", kid)):
            os.mkdir(os.path.join(cwd, "docs", kid))

        path = os.path.join(cwd, "docs", kid, filename)
        dirpath = os.path.join(cwd, "docs", kid)

        self.logger.info('Saving txt %s', path)

        # Write the text file

        with open(path, 'wb') as file:
            file.write(response.body)

        # read downloaded text file and convert to postgres ready text file
        lines = []
        with open(path, 'r') as in_file:
            stripped = [line.strip().strip('"') for line in in_file]
            
            for l in stripped[1:-1]: # Ignore header
                lines.append(api + ';' + kid + ';' + l.replace('","', ';') + '\n')
            lines.append(api + ';' + kid + ';' + stripped[-1].replace('","', ';'))
            
        with open(path, 'w') as out_file:
            out_file.writelines(lines)
        
        # Save to database

        with open(path, 'r') as f:
            # next(f) # Skip the header row.
            DATABASE.cur.copy_from(f, 'oilProduction', sep=';')

        DATABASE.conn.commit()

