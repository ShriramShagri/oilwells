import scrapy
from scrapy.http import FormRequest, Request
from ..items import DSTItem
from ..constants import *

page = 1

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
        global page

        #  Fill the main form Below
        page = 1
        return FormRequest.from_response(response, formdata={
            'ew': 'W',
            'f_st': '15',
            'f_c': str(COUNTY[0]),
            'f_pg': str(page)
        }, callback=self.start_scraping, meta={'index': 0})

    def start_scraping(self, response):
        '''
        This function is used to collect all links in the column per page and iterate through all of them
        '''
        global page
        index = response.meta.get('index')
        wellColumn = set(response.css("tr~ tr+ tr td+ td a::attr(href)").extract())

        # Itetrate Through All links per page
        self.logger.info('No. of links: %s', len(wellColumn))
        if len(wellColumn) > 1:
            for a in wellColumn:
                if len(a) < 80:
                    kid = a.split('=')[-1]
                    if 'DisplayDST' in a:
                        yield response.follow(a,
                                              callback=self.getDST,
                                              meta={'kid': kid, 'index': index})
                    elif 'AcoLinks' in a:
                        yield response.follow(a,
                                              callback=self.getPDF,
                                              meta={'kid': kid, 'index': index})
            page += 1
            yield response.follow(
                f"https://chasm.kgs.ku.edu/ords/dst.dst2.SelectWells?f_t=&f_r=&ew=&f_s=&f_l=&f_op=&f_st=15&f_c={COUNTY[index]}&f_api=&sort_by=&f_pg={page}",
                callback=self.start_scraping,
                meta={'index': index})
        elif index < len(COUNTY) - 1:
            index += 1
            page = 1
            yield response.follow(
                f"https://chasm.kgs.ku.edu/ords/dst.dst2.SelectWells?f_t=&f_r=&ew=&f_s=&f_l=&f_op=&f_st=15&f_c={COUNTY[index]}&f_api=&sort_by=&f_pg={page}",
                callback=self.start_scraping,
                meta={'index': index})

        # yield response.follow('https://chasm.kgs.ku.edu/ords/dst.dst2.DisplayDST?f_kid=1006170157',
        #         callback=self.getDST,
        #         meta={'kid': '1006170161' })

    def findAPI(self, res):
        data = res.css('hr+ table td:nth-child(1)::text').extract()
        temp = data[1].replace('\n', '')
        if temp == "":
            temp = "NO_API"
        return temp

    def getDST(self, response):
        kid = response.meta.get('kid')
        index = response.meta.get('index')
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
                        'kid': kid, 'api': api, 'filename': l.split('/')[-1], 'index': index}
                )

        yield self.items

    def getPDF(self, response):
        kid = response.meta.get('kid')
        index = response.meta.get('index')
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
                    'kid': kid, 'api': api, 'filename': f"DST Report_{count}." + l.split('.')[-1], 'index': index}
            )

    def save_file(self, response):
        '''
        This function is used for downloading files
        '''
        # Get filename from metadata

        filename = response.meta.get('filename')
        kid = response.meta.get('kid')
        api = response.meta.get('api')
        ind = response.meta.get('index')

        # Setup appropriate path and create directory
        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[ind]))):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[ind])))

        if not os.path.isdir(os.path.join(STORAGE_PATH, str(COUNTY[ind]), kid + '_' + api)):
            os.mkdir(os.path.join(STORAGE_PATH, str(COUNTY[ind]), kid + '_' + api))

        path = os.path.join(STORAGE_PATH, str(COUNTY[ind]), kid + '_' + api, filename)

        # Save the file

        self.logger.info('Saving PDF %s', path)
        with open(path, 'wb') as file:
            file.write(response.body)
