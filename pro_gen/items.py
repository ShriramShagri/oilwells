# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProGenItem(scrapy.Item):
    # define the fields for your item here like:
    wh = scrapy.Field()
    ip = scrapy.Field()
    casing = scrapy.Field()
    pf = scrapy.Field()
    cutting = scrapy.Field()
    api = scrapy.Field()
    kid = scrapy.Field()
    pfHeaders = scrapy.Field()
    tops = scrapy.Field()
    dst = scrapy.Field()

    def __str__(self):
        return ""
