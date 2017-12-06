import math
import subprocess

import scrapy
from pydispatch import dispatcher
from scrapy import signals

from amazon.helper import Helper
from amazon.items import ReviewDetailItem, ReviewProfileItem
from amazon.sql import ReviewSql


class ReviewSpider(scrapy.Spider):
    name = 'review_detail'
    custom_settings = {
        'LOG_LEVEL': 'ERROR',
        'LOG_FILE': 'review_detail.json',
        'LOG_ENABLED': True,
        'LOG_STDOUT': True
    }

    def __init__(self, asin, daily=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.asin = asin
        self.last_review = 0
        self.profile_update_self = False    # count profile profile update
        self.updated = False   # determine if profile was updated
        self.daily = True if int(daily) == 1 else False  # determine whether update everyday
        self.start_urls = [
            'https://www.amazon.com/product-reviews/%s?sortBy=recent&filterByStar=three_star' % self.asin,
            'https://www.amazon.com/product-reviews/%s?sortBy=recent&filterByStar=two_star' % self.asin,
            'https://www.amazon.com/product-reviews/%s?sortBy=recent&filterByStar=one_star' % self.asin
        ]
        dispatcher.connect(self.update_profile_self, signals.engine_stopped)
        dispatcher.connect(self.init_profile, signals.engine_started)

    def start_requests(self):
        self.load_profile()
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_detail)

    def parse(self, response):
        reviews = response.css('.review-views .review')
        for row in reviews:
            item = ReviewDetailItem()
            item['asin'] = self.asin
            item['review_id'] = row.css('div::attr(id)')[0].extract()
            item['reviewer'] = row.css('.author::text')[0].extract()
            item['title'] = row.css('.review-title::text')[0].extract()
            item['review_url'] = row.css('.review-title::attr(href)')[0].extract()
            item['date'] = Helper.get_date_split_str(row.css('.review-date::text')[0].extract())
            item['star'] = Helper.get_star_split_str(row.css('.review-rating span::text')[0].extract())
            content = row.css('.review-data .review-text::text').extract()
            item['content'] = '<br />'.join(content) if len(content) > 0 else ''
            yield item

    def get_detail(self, response):
        # get pages 
        page = response.css('ul.a-pagination li a::text')

        i = 1
        # get the amount of reviews
        total = response.css('.AverageCustomerReviews .totalReviewCount::text').extract()  
        # extract reviews
        now_total = Helper.get_num_split_comma(total[0])
        last_review = self.last_review
        sub_total = int(now_total) - int(last_review)
        if sub_total != 0:
            # if sub_total != 0:  
            # if the total !=0 ,then indicate theres new reviews,then update profile 
            self.updated = True
            yield scrapy.Request('https://www.amazon.com/product-reviews/%s' % self.asin,
                                 callback=self.profile_parse)
            if len(page) < 3:  
                #if a < 3 , then there is only 1 page data
                
                yield scrapy.Request(url=response.url + '&pageNumber=1', callback=self.parse)
            else:
                if self.daily:
                    page_num = math.ceil(sub_total / 10)
                    print('update item page_num is %s' % page_num)
                else:
                    self.profile_update_self = True
                    page_num = Helper.get_num_split_comma(page[len(page) - 3].extract())  
                    # count total pages
                while i <= int(page_num):
                    yield scrapy.Request(url=response.url + '&pageNumber=%s' % i,
                                         callback=self.parse)
                    i = i + 1
        else:
            print('there is no item to update')

    def profile_parse(self, response):
        item = ReviewProfileItem()

        item['asin'] = self.asin
        # average score
        average = response.css('.averageStarRatingNumerical a span::text').extract()  
        # exteact average score 
        
        item['review_rate'] = Helper.get_star_split_str(average[0])  
        # toal reviews
        total = response.css('.AverageCustomerReviews .totalReviewCount::text').extract()  
        
        item['review_total'] = Helper.get_num_split_comma(total[0])
        # product name
        product = response.css('.product-title h1 a::text').extract()
        item['product'] = product[0]
        # product  brand
        item['brand'] = response.css('.product-by-line a::text').extract()[0]
        item['image'] = response.css('.product-image img::attr(src)').extract()[0]

        # product seller 
        item['seller'] = item['brand']
        # calculate percentage 
        review_summary = response.css('.reviewNumericalSummary .histogram '
                                      '#histogramTable tr td:last-child').re(r'\d{1,3}\%')

        pct = list(map(lambda x: x[0:-1], review_summary))

        item['pct_five'] = pct[0]
        item['pct_four'] = pct[1]
        item['pct_three'] = pct[2]
        item['pct_two'] = pct[3]
        item['pct_one'] = pct[4]

        yield item

    def load_profile(self):
        # if no profile record, the scrawl new profile and put it into the database 
        
        if self.last_review is False:
            self.profile_update_self = True
            print('this asin profile is not exist, now to get the profile of asin:', self.asin)
            yield scrapy.Request('https://www.amazon.com/product-reviews/%s' % self.asin,
                                 callback=self.profile_parse)
            self.last_review = ReviewSql.get_last_review_total(self.asin)

    # if the profile was recorded the 1st insert lastest_review=0 preventing duplicated，stop running
    def update_profile_self(self):
        if self.profile_update_self is True and self.updated is False:
            # if needed update by it self but not updated yet
            ReviewSql.update_profile_self(self.asin)

    # get latest_review for now
    def init_profile(self):
        self.last_review = ReviewSql.get_last_review_total(self.asin)
