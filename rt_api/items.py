# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

from scrapy.loader.processors import SelectJmes, Compose, MapCompose, Join, TakeFirst


def date_conv(txt):
    return f"{txt[8:10]}-{txt[5:7]}-{txt[:4]}"

def number_days(num_days):
    return int(num_days)-1

def category_to_int():
    pass

class RainbowItem(scrapy.Item):
    provider = scrapy.Field(
        output_processor = TakeFirst()
    )
    country = scrapy.Field(
        output_processor = TakeFirst()
    )
    destination = scrapy.Field(
        output_processor = TakeFirst())
    hotel_name = scrapy.Field(
        output_processor = TakeFirst()
    )
    category = scrapy.Field(
        output_processor = TakeFirst()
    )
    date = scrapy.Field(
        input_processor = MapCompose(date_conv),
        output_processor = TakeFirst()
    )
    airport = scrapy.Field(
        output_processor = TakeFirst()
    )
    board = scrapy.Field(
        output_processor = TakeFirst()
    )
    price = scrapy.Field(
        output_processor = TakeFirst()
    )
    num_days = scrapy.Field(
        input_processor = MapCompose(number_days),
        output_processor = TakeFirst()
    )
    
  