import requests
import pandas as pd
import csv
from pymongo import MongoClient


class DataCrawl(object):
    def __init__(self):
        self.filename = 'ticket.csv'
        self.existed_header = False
        # 建立Mongodb数据库连接
        client = MongoClient(host='localhost', port=27017)
        # 指定连接数据库Ticket
        db = client.Ticket
        # 指定集合名ticket，相当于表名
        self.collection = db.ticket

    def get_data(self):
        # 从city_data.csv文件中获取城市名
        cities = list(pd.read_csv('city_data.csv')['city'])
        for city in cities:
            print('正在爬取%s城市数据' % city)
            # 获取各个城市第一页的网址
            url = "https://travelsearch.fliggy.com/async/queryItemResult.do?" \
                  "searchType=product&keyword=%s&category=SCENIC&pagenum=1" % city

            resp = requests.get(url)
            # 各城市的第一页数据转换成json格式
            result = resp.json()
            # 获取itemPagenum字段
            itemPagenum = result['data']['data'].get('itemPagenum')

            if itemPagenum is not None:
                # 如果itemPagenum字段不为空，获取该城市旅游景点页数page_count
                page_count = itemPagenum['data']['count']
                # 获取itemProducts下的auctions字段内容，
                #                           格式为 [{
                # 							'trip_main_busness_type': 'scenic',
                # 							'src': 'mix',
                # 							'fields': {......}}]
                data_list = result['data']['data']['itemProducts']['data']['list'][0]['auctions']
                # 遍历列表对象data_list，返回结果是dict对象
                for ticket in data_list:
                    # 用items()方法获取dict对象ticket的key、value
                    for ticket_key,ticket_value in ticket.items():
                         # 如果key 为 fields ，则对应的value值为dict对象，即需要的数据
                        if ticket_key == 'fields':
                            # 给ticket_value对象添加一组数据，即对应的城市
                            ticket_value['city'] = city
                            # 将获取的数据上传到mongodb数据库中
                            self.collection.insert_one(ticket_value)
                            # 把ticket_value的数据以DictWriter的方式保存到csv文件中
                            with open(self.filename, 'a', encoding='utf8') as f:
                                writer = csv.DictWriter(f, fieldnames=ticket_value.keys())
                                if not self.existed_header:
                                    writer.writeheader()
                                    self.existed_header = True
                                writer.writerow(ticket_value)
                print('爬取%s城市第%s页成功' % (city, 1))
                # 如果该城市的旅游景点页数大于一页
                if page_count > 1:
                    for page in range(2, page_count+1):
                        # 获取该城市所有页面的url
                        url = "https://travelsearch.fliggy.com/async/queryItemResult.do?" \
                              "searchType=product&keyword=%s&category=SCENIC&pagenum=%s"%(city,page)
                        resp = requests.get(url)
                        result = resp.json()
                        data_list = result['data']['data']['itemProducts']['data']['list'][0]['auctions']

                        for ticket in data_list:
                            for ticket_key, ticket_value in ticket.items():
                                if ticket_key == 'fields':

                                    ticket_value['city'] = city
                                    # 将获取的数据上传到mongodb数据库中
                                    self.collection.insert_one(ticket_value)
                                    with open(self.filename, 'a', encoding='utf8') as f:
                                        writer = csv.DictWriter(f, fieldnames=ticket_value.keys())
                                        if not self.existed_header:
                                            writer.writeheader()
                                            self.existed_header = True
                                        writer.writerow(ticket_value)
                        print('爬取%s城市第%s页成功' % (city, page))


if __name__ == '__main__':
    datacrawl = DataCrawl()
    datacrawl.get_data()
