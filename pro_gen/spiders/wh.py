import scrapy
from scrapy.http import FormRequest, Request
from ..items import ProGenItem
from ..constants import *
from ..essentials import *
import xlrd
import csv

class Crawler(scrapy.Spider):
    name = CRAWLER_NAME['wh']
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Qualified/index.html'
    ]

    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        for i in COUNTY:
            if not os.path.exists(os.path.join(STORAGE_PATH, str(i))):
                os.mkdir(os.path.join(STORAGE_PATH, str(i)))
            yield response.follow(
                f'https://chasm.kgs.ku.edu/ords/qualified.ogw5.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={i}&f_ws=ALL&f_api=&sort_by=&f_pg=1',
                callback=self.start_scraping,
                meta={'index': i, 'page' : 1})
    
        # for i in COUNTY:
        # yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1046867289',
        #                         callback=self.get_data,
        #                         meta = {'index' : 19})

        # yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1044632327',
        #                         callback=self.get_tables,
        #                         meta = {'api' : 19, 'kid' : '1044632327'})


    def start_scraping(self, response):

        index = response.meta.get('index')
        page = response.meta.get('page')

        wellColumn = response.css("tr~ tr+ tr a:nth-child(1)::attr(href)").extract()

        # Itetrate Through All links per page
        self.logger.info('No. of links: %s', len(wellColumn))
        if len(wellColumn) > 1:
            for a in wellColumn:
                if len(a)<80:
                    yield response.follow(a,
                                callback=self.get_data,
                                meta = {'index' : index})

            yield response.follow(
                    f"https://chasm.kgs.ku.edu/ords/qualified.ogw5.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={index}&f_ws=ALL&f_api=&sort_by=&f_pg={page+1}",
                callback=self.start_scraping,
                meta = {'index' : index, 'page' : page + 1})
    
    def get_data(self, response):
        self.items = ProGenItem()

        county = response.meta.get('index')

        # <---------------------------------------------WH---------------------------------------------------->
        CURRENTKID, CURRENTAPI = '', ''
        well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()
        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()
        if 'API: ' in well_data or 'KID: ' in well_data:
            self.items['wh'] = well_data
            CURRENTAPI = well_data[well_data.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data[well_data.index('KID: ') + 1].replace("\n", "")
        elif 'API: ' in well_data2 or 'KID: ' in well_data2:
            self.items['wh'] = well_data2
            CURRENTAPI = well_data2[well_data2.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data2[well_data2.index('KID: ') + 1].replace("\n", "")

        # <------------------------------------------------------------------------------------------------->


        headers = response.css('h3::text').extract()

        # <------------------------------------------Cuttings------------------------------------------------------->
        if "Cuttings Data" in headers:
            try:
                error = [False, '']
                possibleCss = ['table:nth-child(6)', 'table:nth-child(2)', 'table:nth-child(4)', 'table:nth-child(8)', 'td table~ table', 'table:nth-child(10)']

                for c in possibleCss:
                    cutting = response.css(c).extract()
                    if len(cutting) > 0:
                        if 'Box Number:' in cutting[-1]:
                            self.items['cutting'] = cutting
                            error = [False, '']
                        else:
                            error = [True, "No 'Box Number:' in the retrived list"]
                    else:
                        error = [True, "Len of table = 0"]
                    if not error[0]:
                        break
                if error[0]:
                    # Still errors :(
                    self.error(CURRENTAPI, CURRENTKID, error[1], 'cutting')

            except Exception as err:
                self.error(CURRENTAPI, CURRENTKID, str(err), 'cutting')

        # <---------------------------------------------Survey Data---------------------------------------------------->
           
        # Download Sources
        getLinks = response.css('a::attr(href)').extract()[3:]
        getLinkNames = response.css('a::text').extract()
        count = 0
        sourceDownloaded = False
        for lnk in getLinkNames:
            if lnk in DOWNLOAD_CHECK:
                if (lnk == SOURCES or lnk.split('.')[-1] == 'xlsx') and DOWNLOAD['sources']:
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county))):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county)))
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID)):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID))
                    # Download and save xlsx
                    count += 1
                    sourceDownloaded = True
                    yield Request(
                        url=response.urljoin(getLinks[getLinkNames.index(lnk)]),
                        callback=self.saveExcel,
                        meta={
                            'filename': os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID, 'Sources_' + str(count) + "." + getLinks[getLinkNames.index(lnk)].split('.')[-1]), 'ext' : getLinks[getLinkNames.index(lnk)].split('.')[-1]
                            ,'api': CURRENTAPI, 'kid': CURRENTKID} )

                # <----------------------------------------Oil Production--------------------------------------------------------->

                elif lnk in OIL:
                    count += 1
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county))):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county)))
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID)):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID))
                    # Download and save oil/Gas Production Data
                    yield response.follow(
                    getLinks[getLinkNames.index(lnk)].replace('.MainLease?', '.MonthSave?'),
                    callback=self.getOilData,
                    meta={'kid': CURRENTKID, 'api': CURRENTAPI, 'filename' : os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID, f'production_{count}.txt')})
                
                # <-------------------------------------------Pdfs------------------------------------------------------>

                elif lnk in TODOWNLOAD and DOWNLOAD['pdf']:
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county))):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county)))
                    if not os.path.exists(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID)):
                        os.mkdir(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID))
                    # Download and save all pdfs
                    count += 1
                    yield Request(
                        url=response.urljoin(getLinks[getLinkNames.index(lnk)]),
                        callback=self.save_file,
                        meta={
                            'filename': os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID, lnk + str(count) + "." + getLinks[getLinkNames.index(lnk)].split('.')[-1]),
                            }
                    )

        # <--------------------------------------------Check again for survey data----------------------------------------------------->
        if not sourceDownloaded:
            link = response.css('br+ table tr+ tr a::attr(href)').extract()
            linktext = response.css('br+ table tr+ tr a::text').extract()

            if type(linktext) is list and type(link) is list:
                if len(link) > 1:
                    if (link[1].split('.')[-1] == 'xlsx' or linktext[0] == SOURCES) and DOWNLOAD['sources']:

                        if not os.path.exists(os.path.join(STORAGE_PATH, str(county))):
                                os.mkdir(os.path.join(STORAGE_PATH, str(county)))

                        if not os.path.exists(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID +"CAUGHT")):
                            os.mkdir(os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID+"CAUGHT"))

                        # Download and save xlsx
                        count += 1
                        sourceDownloaded = True
                        yield Request(
                            url=response.urljoin(link),
                            callback=self.saveExcel,
                            meta={
                                'filename': os.path.join(STORAGE_PATH, str(county), CURRENTAPI + "_" + CURRENTKID+"CAUGHT", 'Sources_' + str(count) + "." + link.split('.')[-1]), 'ext' : link.split('.')[-1]
                                ,'api': CURRENTAPI, 'kid': CURRENTKID} )

        # <------------------------------------------------------------------------------------------------->

        # # Check if Engineering Data Page is present
        toggleEngineering = response.css("hr+ table a").xpath("@href").extract()
        toggleEngineeringText = response.css("hr+ table a::text").extract()
        self.items['api'], self.items['kid'] = CURRENTAPI, CURRENTKID

        # <------------------------------------------------------------------------------------------------->

        yield self.items

        # <----------------------------------------------Engg Data--------------------------------------------------->

        if toggleEngineeringText:
            if toggleEngineeringText[0] == "View Engineering Data" and toggleEngineering:
                yield response.follow(toggleEngineering[0], callback=self.get_tables,
                                      meta={'api': CURRENTAPI, 'kid': CURRENTKID, 'county': county})
                        
    def get_tables(self, response):
        '''
        This function is used to Data from engineering page
        '''
        self.items = ProGenItem()

        CURRENTKID, CURRENTAPI = '', ''

        CURRENTKID, kid = response.meta.get('kid'), response.meta.get('kid')
        CURRENTAPI, api = response.meta.get('api'), response.meta.get('api')

        self.logger.info('Switched to Engineering Data: KID= %s', CURRENTKID)

        self.items['api'], self.items['kid'] = api, kid

        # Get All H3 tags to recognise all headings inside tables in the page
        headers = response.css('td h3::text').extract()

        # <-------------------------------------------Casing------------------------------------------------------>
        if "Casing record" in headers:
            # Collect casing table and Send to Pipeline (Multiple css elector path might be present)
            casing_data = response.css("table:nth-child(9) ::text").extract()
            if casing_data:
                if 'Casing record' in casing_data:
                    self.items['casing'] = casing_data
                else:
                    self.error(CURRENTAPI, CURRENTKID, 'Wrong Table', 'casing')
            else:
                self.error(CURRENTAPI, CURRENTKID, 'Table missing', 'casing')

        # <------------------------------------------Perforation------------------------------------------------------->
        if "Perforation Record" in headers:
            # Collect pf table and Send to Pipeline (Multiple css elector path might be present)
            try:
                perforationHeaders = response.css('table:nth-child(13) th').extract()
                perforation_data = response.css("table:nth-child(13) tr+ tr td").extract()
                if '<th>Shots Per Foot</th>' in perforationHeaders:
                    self.items['pfHeaders'] = perforationHeaders
                    self.items['pf'] = perforation_data
                else:
                    self.error(CURRENTAPI, CURRENTKID, 'Wrong Table', 'pf')
            except Exception as err:
                self.error(CURRENTAPI, CURRENTKID, str(err), 'pf')

        # <------------------------------------------IP------------------------------------------------------->
        # Collect IP table and Send to Pipeline (Multiple css elector path might be present)
        initial_potential = response.css('table:nth-child(7) td ::text').extract()
        if initial_potential:
            if '\nProducing Method: ' in initial_potential:
                self.items['ip'] = initial_potential
            else:
                self.error(CURRENTAPI, CURRENTKID, 'Wrong Table or data missing', 'ip')
        else:
            self.error(CURRENTAPI, CURRENTKID, 'Table missing', 'ip')

        yield self.items

    def saveExcel(self, response):
        kid = response.meta.get('kid')
        api = response.meta.get('api')
        filename = response.meta.get('filename')
        ext = response.meta.get('ext')
        with open(filename, 'wb') as file:
            file.write(response.body)
        if ext == 'xlsx':
            wb = xlrd.open_workbook(filename)
            sh = wb.sheet_by_name('page 1')
            your_csv_file = open(os.path.join(os.path.dirname(filename), 'testcsv.csv'), 'w', newline='')
            wr = csv.writer(your_csv_file)
            for rownum in range(sh.nrows):
                wr.writerow(sh.row_values(rownum))
            your_csv_file.close()
            try:
                with open(os.path.join(os.path.dirname(filename), 'testcsv.csv'), 'r') as f:
                    next(f) # Skip the header row.
                    DATABASE.cur.copy_from(f, 'st', sep=',')
            except Exception as e:
                DATABASE.conn.rollback()
                sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
                DATABASE.cur.execute(sql, (api, kid, str(e), "st"))
                DATABASE.conn.commit()
            else:
                DATABASE.conn.commit()
            os.remove(os.path.join(os.path.dirname(filename), 'testcsv.csv'))
    
    def save_file(self, response):
        '''
        This function is used for downloading files
        '''
        # Get filename from metadata
        filename = response.meta.get('filename')
        # Save the file
        self.logger.info('Saving PDF %s', filename)
        with open(filename, 'wb') as file:
            file.write(response.body)

    def getOilData(self, response):
        kid = response.meta.get('kid')
        api = response.meta.get('api')
        filename = response.meta.get('filename')
        #  Get txt file link and download
        oil_data_href = response.css('a').xpath('@href').extract()
        for item in oil_data_href:
            if item.split('.').pop() == 'txt':
                yield Request(
                url=response.urljoin(oil_data_href[-1]),
                callback=self.save_oil_data,
                meta={'filename': filename, 'kid': kid, 'api': api}
            )
    
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
        self.logger.info('Saving txt %s', filename)
        # Write the text file
        with open(filename, 'wb') as file:
            file.write(response.body)
        # read downloaded text file and convert to postgres ready text file
        lines = []
        with open(filename, 'r') as in_file:
            stripped = [line.strip().strip('"') for line in in_file]
            for l in stripped[1:-1]:  # Ignore header
                lines.append(api + ';' + kid + ';' + l.replace('","', ';') + '\n')
            lines.append(api + ';' + kid + ';' + stripped[-1].replace('","', ';'))
        with open(filename, 'w') as out_file:
            out_file.writelines(lines)
        # Save to database
        try:
            with open(filename, 'r') as f:
                # next(f) # Skip the header row.
                DATABASE.cur.copy_from(f, 'oilProduction', sep=';')
        except Exception as e:
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "oilproduction"))
            DATABASE.conn.commit()
            if not DOWNLOAD['oilProduction']:
                os.remove(filename)
        else:
            DATABASE.conn.commit()
            if not DOWNLOAD['oilProduction']:
                os.remove(filename)

    def error(self, api, kid, e, table):
        sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
        DATABASE.cur.execute(sql, (api, kid, e, table))
        DATABASE.conn.commit()
