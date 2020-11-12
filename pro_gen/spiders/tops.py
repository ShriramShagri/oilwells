import scrapy
from scrapy.http import FormRequest, Request
from ..items import ProGenItem
from ..constants import *

class Crawler(scrapy.Spider):
    name = CRAWLER_NAME['tops']
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Tops/index.html'
    ]

    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        for i in COUNTY:
            yield response.follow(
                f'https://chasm.kgs.ku.edu/ords/qualified.tops_d.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={i}&f_api=&sort_by=&f_pg=1',
                callback=self.start_scraping,
                meta={'index': i, 'page' : 1, 'm' : 0})
    
        # for i in COUNTY:
        # yield response.follow('https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1031431227',
        #                         callback=self.get_data,
        #                         meta = {'index' : 19})

    def start_scraping(self, response):

        index = response.meta.get('index')
        page = response.meta.get('page')
        multi = response.meta.get('m')
        # wellColumn = None

        if multi == 0:
            test1 = response.css("tr~ tr+ tr a:nth-child(1)::attr(href)").extract()
            test2 = response.css("a::attr(href)").extract()[13:-1]
            if len(test1) == 50:
                wellColumn = test1
                multi = 1
            else:
                wellColumn = test2
                multi = 2
        
        elif multi == 1:
            wellColumn = response.css("tr~ tr+ tr a:nth-child(1)::attr(href)").extract()
        
        elif multi == 2:
            wellColumn = response.css("a::attr(href)").extract()[13:-1]

        # Itetrate Through All links per page 
        self.logger.info('No. of links: %s', len(wellColumn)) # Ignore unbound warning It'll always be bound
        if len(wellColumn) >= 1: # Ignore unbound warning It'll always be bound
            for a in wellColumn: # Ignore unbound warning It'll always be bound
                if len(a)<80:
                    yield response.follow(a,
                                callback=self.get_data)

            yield response.follow(
                    f"https://chasm.kgs.ku.edu/ords/qualified.tops_d.SelectWells?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c={index}&f_ws=ALL&f_api=&sort_by=&f_pg={page+1}",
                callback=self.start_scraping,
                meta = {'index' : index, 'page' : page + 1, 'm' : multi})
    
    def get_data(self, response):
        self.items = ProGenItem()

        CURRENTKID, CURRENTAPI = '', ''
        well_data = response.css('hr+ table tr:nth-child(1) ::text').extract()
        well_data2 = response.css("table:nth-child(5) tr:nth-child(1) ::text").extract()
        if 'API: ' in well_data or 'KID: ' in well_data:
            CURRENTAPI = well_data[well_data.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data[well_data.index('KID: ') + 1].replace("\n", "")
        elif 'API: ' in well_data2 or 'KID: ' in well_data2:
            CURRENTAPI = well_data2[well_data2.index('API: ') + 1].replace("\n", "")
            CURRENTKID = well_data2[well_data2.index('KID: ') + 1].replace("\n", "")
        
        getLinks = response.css('a::attr(href)').extract()[3:]
        getLinkNames = response.css('a::text').extract()
        self.items['api'] = CURRENTAPI
        self.items['kid'] = CURRENTKID

        if 'View tops for this well' in getLinkNames:
            print(getLinks[getLinkNames.index('View tops for this well')])
            yield Request(
                url=getLinks[getLinkNames.index('View tops for this well')],
                callback=self.getTopsTable,
                meta={ 'api': CURRENTAPI, 'kid': CURRENTKID} 
                )
            # Formation or Top or Base
        else:
            try:
                error = [False, '']
                possibleCss = ['table:nth-child(8)', 'table:nth-child(6)', 'table:nth-child(12)', 'table:nth-child(10)', 'table:nth-child(4)', 'td table', 'td td table']
                for c in possibleCss:
                    table = response.css(c).extract()
                    if len(table) > 0:
                        if 'Formation' in table[-1] and 'Top' in table[-1] and 'Base' in table[-1]:
                            self.items['tops'] = table[-1]
                            error = [False, '']
                        else:
                            error = [True, "No 'Formation' in the retrived list"]
                    else:
                        error = [True, "Len of table = 0"]
                    if not error[0]:
                        break
                if error[0]:
                    # Still errors :(
                    self.error(CURRENTAPI, CURRENTKID, error[1], 'tops')
            except Exception as err:
                    self.error(CURRENTAPI, CURRENTKID, str(err), 'tops')
            
            else:
                yield self.items

    def getTopsTable(self, response):
        self.items = ProGenItem()

        self.items['api'] = response.meta.get('api')
        self.items['kid'] = response.meta.get('kid')

        table = response.css('hr+ table').extract()
        try:
            if 'Formation' in table[-1] or 'Top' in table[-1] or 'Base' in table[-1]:
                self.items['tops'] = table[-1]
                yield self.items
            else:
                self.error(response.meta.get('api'), response.meta.get('kid'), 'No Table in follow up link', 'tops')
        except Exception as err:
                self.error(response.meta.get('api'), response.meta.get('kid'), str(err), 'tops')
    
    def error(self, api, kid, e, table):
        sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
        DATABASE.cur.execute(sql, (api, kid, e, table))
        DATABASE.conn.commit()