import scrapy
from scrapy.http import FormRequest, Request
from ..items import ProGenItem
from ..constants import *
from ..essentials import *

class Crawler(scrapy.Spider):
    name = CRAWLER_NAME['production']
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/Field/lease.html'
    ]

    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        for i in COUNTY:
            if not os.path.exists(os.path.join(STORAGE_PATH, str(i))):
                os.mkdir(os.path.join(STORAGE_PATH, str(i)))
            yield response.follow(
                f'https://chasm.kgs.ku.edu/ords/oil.ogl5.SelectLeases?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_c={i}&sort_by=&f_pg=1',
                callback=self.start_scraping,
                meta={'index': i, 'page' : 1, 'm' : 0})
    
        # for i in COUNTY:
        # yield response.follow('https://chasm.kgs.ku.edu/ords/oil.ogl5.SelectLeases?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_c=3&sort_by=&f_pg=2',
        #                         callback=self.start_scraping,
        #                         meta = {'index' : 19})

    def start_scraping(self, response):
        self.items = ProGenItem()
        index = response.meta.get('index')
        page = response.meta.get('page')

        self.items['api'], self.items['kid'] = 'NA', 'NA'

        wellColumn = response.css("tr+ tr td:nth-child(1) a::attr(href)").extract()

        # # Itetrate Through All links per page
        self.logger.info('No. of links: %s', len(wellColumn))
        if len(wellColumn) > 2:
            datacol1 = response.css('tr+ tr td:nth-child(1)').extract()
            datacol2 = response.css('tr+ tr td:nth-child(2)').extract()
            datacol3 = response.css('tr+ tr td:nth-child(3)').extract()
            datacol4 = response.css('td:nth-child(4)').extract()
            self.items['production'] = [datacol1, datacol2, datacol3, datacol4]
            yield self.items
            for ind, a in enumerate(wellColumn):
                if len(a)<80:
                    yield response.follow(a,
                                callback=self.get_data,
                                meta = {'index' : index,  'filename' : os.path.join(STORAGE_PATH, str(index), f'production_{page}_{ind}.txt')}
                                )
            yield response.follow(
                f'https://chasm.kgs.ku.edu/ords/oil.ogl5.SelectLeases?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_c={index}&sort_by=&f_pg={page+1}',
                callback=self.start_scraping,
                meta = {'index' : index, 'page' : page + 1})
    
    def get_data(self, response):
        index = response.meta.get('index')
        filename = response.meta.get('filename')

        fileLink = response.css("a::attr(href)").extract()[3]

        yield response.follow(fileLink,
                callback=self.getOilData,
                meta = {'index' : index, 'filename' : filename,})
    
    def getOilData(self, response):
        # kid = response.meta.get('kid')
        # api = response.meta.get('api')
        filename = response.meta.get('filename')
        count = response.meta.get('m')
        #  Get txt file link and download
        oil_data_href = response.css('a').xpath('@href').extract()
        for item in oil_data_href:
            if item.split('.').pop() == 'txt':
                yield Request(
                url=response.urljoin(oil_data_href[-1]),
                callback=self.save_oil_data,
                meta={'filename': filename, 'api': item}
            )
    def save_oil_data(self, response):
        '''
        This function reads oil production txt file and saves it first
        then writs the content fron text file to csv and saves the contents in postgres
        '''
        # Collect filename and KID from metadata
        filename = response.meta.get('filename')
        # kid = response.meta.get('kid')
        item = response.meta.get('api')
        # if len(api) > 4:
        #     pass
        # else:
        #     api = 'NOAPI'
        # Setup appropriate path and create directory
        # Write the text file
        with open(filename, 'wb') as file:
            file.write(response.body)
        # read downloaded text file and convert to postgres ready text file
        lines = []
        with open(filename, 'r') as in_file:
            stripped = [line.strip().strip('"') for line in in_file]
            for l in stripped[1:-1]:  # Ignore header
                lines.append(item + ';' + 'NA' + ';' + l.replace('","', ';') + '\n')
            lines.append(item + ';' + 'NA' + ';' + stripped[-1].replace('","', ';'))
        self.logger.info('Saving txt %s', filename)
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
            DATABASE.cur.execute(sql, (item, 'NA', str(e), "oilproduction"))
            DATABASE.conn.commit()
            if not DOWNLOAD['oilProduction']:
                os.remove(filename)
        else:
            DATABASE.conn.commit()
            if not DOWNLOAD['oilProduction']:
                os.remove(filename)
        