import scrapy
from scrapy.http import FormRequest, Request
from ..items import DSTItem
from ..constants import *
from ..essentials import *

class Crawler(scrapy.Spider):
    name = CRAWLER_NAME['dst']
    # Start Url
    start_urls = [
        'http://www.kgs.ku.edu/Magellan/DST/index.html'
    ]

    # REPLACE self.start_scrapping BY parse METHOD!!!
    def parse(self, response):
        '''
        Overrided function " Let's Start Scraping!!"
        '''
        for i in COUNTY:
            if not os.path.exists(os.path.join(STORAGE_PATH, str(i))):
                os.mkdir(os.path.join(STORAGE_PATH, str(i)))
            yield response.follow(
                f"https://chasm.kgs.ku.edu/ords/dst.dst2.SelectWells?f_t=&f_r=&ew=&f_s=&f_l=&f_op=&f_st=15&f_c={i}&f_api=&sort_by=&f_pg=1",
                callback=self.start_scraping,
                meta={'index': i, 'page' : 1})


    def start_scraping(self, response):
        '''
        This function is used to collect all links in the column per page and iterate through all of them
        '''
        index = response.meta.get('index')
        page = response.meta.get('page')
        wellColumn = set(response.css("tr~ tr+ tr td+ td a::attr(href)").extract())

        # Itetrate Through All links per page
        self.logger.info('No. of links: %s', len(wellColumn))
        if len(wellColumn) > 1:
            for a in wellColumn:
                kid = a.split('=')[-1]
                if 'DisplayDST' in a:
                    yield response.follow(a,
                                            callback=self.getDST,
                                            meta={'kid': kid, 'index': index})
                elif 'AcoLinks' in a:
                    yield response.follow(a,
                                            callback=self.getPDF,
                                            meta={'kid': kid, 'index': index})
            yield response.follow(
                f"https://chasm.kgs.ku.edu/ords/dst.dst2.SelectWells?f_t=&f_r=&ew=&f_s=&f_l=&f_op=&f_st=15&f_c={index}&f_api=&sort_by=&f_pg={page+1}",
                callback=self.start_scraping,
                meta={'index': index, 'page' : page+1})

    def findAPI(self, res):
        data = res.css('hr+ table td:nth-child(1)::text').extract()
        temp = data[1].replace('\n', '')
        if temp == "":
            temp = "NO_API"
        return temp

    def getDST(self, response):
        kid = response.meta.get('kid')
        api = self.findAPI(response)
        self.items = DSTItem()
        self.items['kid'] = kid
        self.items['api'] = api

        # Table Data
        self.items['table'] = response.css('table+ table td').extract()

        # Downloads
        downloadLinks = response.css('tr+ tr a::attr(href)').extract()
        downloadLinkText = response.css('tr+ tr a::text').extract()
        for index, text in enumerate(downloadLinkText):
            if text == 'Download original data':
                l = downloadLinks[index]
                yield Request(
                    url=response.urljoin(l),
                    callback=self.save_file,
                    meta={
                        'kid': kid, 'api': api, 'filename': l.split('/')[-1], 'index': response.meta.get('index')}
                )

        yield self.items

    def getPDF(self, response):
        kid = response.meta.get('kid')
        api = self.findAPI(response)

        filelinks = response.css('li a::attr(href)').extract()

        f = lambda x: True if x.split('.')[-1] in DST_Extensions else False

        filteredLinks = filter(f, filelinks)

        count = 0
        for l in filteredLinks:
            count += 1
            yield Request(
                url=response.urljoin(l),
                callback=self.save_file,
                meta={
                    'kid': kid, 'api': api, 'filename': f"DST Report_{count}." + l.split('.')[-1], 'index': response.meta.get('index')}
            )

    def save_file(self, response):
        '''
        This function is used for downloading files
        '''
        # Get filename from metadata

        filename = response.meta.get('filename')
        kid = response.meta.get('kid')
        api = response.meta.get('api')
        index = response.meta.get('index')

        # Setup appropriate path and create directory

        if not os.path.isdir(os.path.join(STORAGE_PATH, str(index), kid + '_' + api)):
            os.mkdir(os.path.join(STORAGE_PATH, str(index), kid + '_' + api))

        path = os.path.join(STORAGE_PATH, str(index), kid + '_' + api, filename)

        # Save the file

        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)
