import scrapy
from bs4 import BeautifulSoup
import mysql.connector
import json
from slugify import slugify


class PoshmarkbotSpider(scrapy.Spider):
    name = 'poshmarkBot'
    allowed_domains = ['poshmark.com']
    conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
    cursor = conn.cursor(buffered=True)
    start_urls = ['https://poshmark.com/listing/Vintage-Pelican-Basket-62fb96b883cbecc7c7a77e85']

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        xx= soup.find_all('a',{"data-et-name":"subcategory"})
        urls = ["https://poshmark.com"+x.get('href') for x in xx]
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse2,meta={'count':0})

    def parse2(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        urls2 = list(set(["https://poshmark.com"+x.get('href') for x in soup.find_all('a',{'data-et-name':'listing'}) if "/listing/" in str(x)]))
        for url in urls2:
            yield scrapy.Request(url=url, callback=self.parse3)
        if not urls2:
            return
        else:
            yield scrapy.Request(url=response.request.url.split('?')[0]+f"?max_id={response.meta['count']+1}",callback=self.parse2,meta={'count':response.meta['count']+1,'crawl_once':True})


    def parse3(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        content = [x.text for x in soup.find_all('script') if "window.__INITIAL_STATE__=" in x.text][0].split("window.__INITIAL_STATE__=")[-1].split(';(func')[0]
        # with open('test.txt','w', encoding='utf-8') as f:
        #     f.write(content)
        data = json.loads(content)['$_listing_details']['listingDetails']
        for _ in range(10):
            try:
                department = data['catalog']['department']
                brand = data['brand_id']
                category = data['catalog']['category']
                url = f"https://poshmark.com/vm-rest/searches/popular?department_id={department}&category_id={category}&brand_id={brand}&pm_version=215.0.0"
                yield scrapy.Request(url=url,callback=self.parse4, dont_filter=True)
            except KeyError:
                return


    def parse4(self, response):
        data = json.loads(response.body)
        
        if data['data']:
            for d in data['data']:
                try:
                    sql_update_query = f'''SELECT * FROM sql_usedpick_com.all_keywords WHERE keyword = {data['data']};'''
                    self.cursor.execute(sql_update_query)
                    x = self.cursor.fetchone()
                    if not x:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s,%s);'''
                        self.cursor.execute(sql_update_query,(d['keyword'],slugify(d['keyword'])))
                        self.conn.commit()
                except:
                    self.conn = mysql.connector.connect(host='154.38.160.70',
                                    database='sql_usedpick_com',
                                    user='sql_usedpick_com',
                                    password='e5empmmWBjBEr5s6')
                    self.cursor = self.conn.cursor(buffered=True)
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s,%s);'''
                    self.cursor.execute(sql_update_query,(d['keyword'],slugify(d['keyword'])))
                    self.conn.commit()
                yield {"keyword":d['keyword']}
        else:
            return
