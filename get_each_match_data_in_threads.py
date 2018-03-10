# -*- coding: utf-8 -*-

import configparser
import db_connecter
import datetime
import logging
import psycopg2
import Queue
import re
import requests
import signal
import sys
import threading

from lxml import etree
from lxml import html
from logging import config
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from random import randint
from time import sleep

class GetHtmlFailed(Exception):
    pass

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

class GetEachMatchOdds:
    defaultEncoding = 'utf-8'
    
    def __init__(self, loglevel, db_config_file, section_name):
    	self.pool_size = 2
    	self.min_db_conn_size = 2
        logging.config.fileConfig('logger_test.conf')
        self.logger = logging.getLogger(loglevel)
        self.config = configparser.ConfigParser()
        self.config.read(db_config_file)
        self.host = self.config[section_name]['host']
        self.port = self.config[section_name]['port']
        self.user = self.config[section_name]['user']
        self.password = self.config[section_name]['password']
        self.database = self.config[section_name]['database']
        self.conn_pool = ThreadedConnectionPool(self.min_db_conn_size, self.pool_size, host=self.host, port=self.port, user=self.user, database=self.database)
        self.main_db_conn = db_connecter.DBConnecter(db_config_file, section_name)
        self.main_db_conn.connect()
        pass

    def basic_get_match_info_task(self, queue):
        headers = {'User-Agent':'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)', 'Referer':'https://developer.mozilla.org/en-US/docs/Web/JavaScript'}
        euro_sql_str='''insert into euro_odds
        (match_id,
        result,
        company,
        priodds3,
        priodds1,
        priodds0,
        nowodds3,
        nowodds1,
        nowodds0,
        prichance3,
        prichance1,
        prichance0,
        nowchance3,
        nowchance1,
        nowchance0,
        priyrr,
        nowyrr,
        prikelly3,
        prikelly1,
        prikelly0,
        nowkelly3,
        nowkelly1,
        nowkelly0
        )
        values
        (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
        )
        '''
        asia_sql_str='''insert into asia_odds
        (match_id,
        result,
        company,
        priodds3,
        priconcede,
        priodds0,
        nowodds3,
        nowconcede,
        nowodds0
        )
        values
        (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
        )
        '''
        threadname = threading.currentThread().getName()
        while True:
            rows = queue.get()
            db_conn = self.conn_pool.getconn()
            curs = db_conn.cursor()
            for row in rows:
            	while True:
                    try:
                        self.logger.info(threadname + ":" + str([row[0], row[1], row[2], row[3], row[4]]))
                        match_id = row[0]
                        if row[1] > row[2]:
                        	result = 3
                        elif row[1] == row[2]:
                        	result = 1
                        else:
                        	result = 0
                        
                        if re.search(r'^http:', row[3]):
                            asia_data_url = row[3]
                        else:
                        	asia_data_url = 'http:' + row[3]
                        	
                        if re.search(r'^http:', row[4]):
                            euro_data_url = row[4]
                        else:
                        	euro_data_url = 'http:' + row[4]
                        
                        euro_r = requests.get(euro_data_url, headers=headers, timeout=5)
                        euro_r.encoding = euro_r.apparent_encoding
                        sleep(0.5)
                        asia_r = requests.get(asia_data_url, headers=headers, timeout=5)
                        asia_r.encoding = asia_r.apparent_encoding
                        _euro_html = html.fromstring(euro_r.text)
                        if (len(_euro_html.xpath('//table[@class="pl_table_data"]')) > 0):
                            all_euro_odds = self.parse_euro_new_html(euro_r)
                            all_asia_odds = self.parse_asia_new_html(asia_r)
                            
                            for e_odds in all_euro_odds:
                            	curs.execute(euro_sql_str, tuple([match_id, result] + e_odds))

                            for a_odds in all_asia_odds:
                                curs.execute(asia_sql_str, tuple([match_id, result] + a_odds))
                                
                        else:
                            all_euro_odds = self.parse_euro_old_html(euro_r)
                            all_asia_odds = self.parse_asia_old_html(asia_r)
                            
                            for e_odds in all_euro_odds:
                                curs.execute(euro_sql_str, tuple([match_id, result] + e_odds))
                            
                            for a_odds in all_asia_odds:
                                curs.execute(asia_sql_str, tuple([match_id, result] + a_odds))
                            	
                        db_conn.commit()
                    except GetHtmlFailed as err:
                    	self.logger.error("Get Html Failed: {0}. 2 seconds later retry to get....".format(err))
                    	sleep(2)
                    	continue
                    except Exception as err:
                    	self.logger.error("Get Html Failed: {0}. 5 seconds later retry to get....".format(err))
                    	sleep(5)
                    	continue
                    break
            self.conn_pool.putconn(db_conn)
            queue.task_done()
            
    def basic_work(self, group_size, start_offset, end_offset):
    	self.logger.info("Start get matches infomation...")
    	start_time = datetime.datetime.now()
        queue = Queue.Queue(maxsize=self.pool_size)
        killer = GracefulKiller()
        for i in range(self.pool_size):
            t = threading.Thread(target=self.basic_get_match_info_task,name='Task-'+str(i), args=(queue,))
            t.daemon = True
            t.start()
        
        current_offset = start_offset
        headers = {'User-Agent':'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)', 'Referer':'https://developer.mozilla.org/en-US/docs/Web/JavaScript'}
        while(current_offset <= end_offset):
            sql_str = 'select id, home_team_score, away_team_score, asia_data_url, euro_data_url from matchs limit %s offset %s'
            self.main_db_conn.select_record(sql_str, (group_size, current_offset))
            rows = self.main_db_conn.fetchall()
            queue.put(rows)
            
            if killer.kill_now:
                self.logger.info("The programing will stop after all task done, and the current offset is: %s", current_offset+group_size)
                break
            
            current_offset += group_size
        
        if not killer.kill_now:
            self.logger.info("Get matches info completed!")
        
        self.main_db_conn.close()
        queue.join()
        end_time = datetime.datetime.now()
        last_time = end_time - start_time
        self.logger.info("Get matches infomation finished, the program lasted for %s seconds", last_time.total_seconds())

    def parse_euro_new_html(self, response):
        _html = html.fromstring(response.text)

        company_names = _html.xpath('//span[@class="quancheng"]/text()')
        tables = _html.xpath('//table[@class="pl_table_data"]')
        
        if(len(_html.xpath('//table[@class="pl_table_data"]')) == 0):
            raise GetHtmlFailed("Can't get Euro odds")

        if(len(company_names) < 1):
            return []
        
        odds_arr = []
        for tb in tables[3::4]:
            trs = tb.xpath('./tbody/tr')
            pri_odds = trs[0].xpath('./td/text()')
            last_odds = trs[1].xpath('./td/text()')
            odds_arr.append([pri_odds, last_odds])

        chance_arr = []
        for tb in tables[4::4]:
            trs = tb.xpath('./tbody/tr')
            pri_chance = trs[0].xpath('./td/text()')
            last_chance = trs[1].xpath('./td/text()')
            chance_arr.append([pri_chance, last_chance])

        yrr_arr = []
        for tb in tables[5::4]:
            trs = tb.xpath('./tbody/tr')
            pri_yrr = trs[0].xpath('./td/text()')
            last_yrr = trs[1].xpath('./td/text()')
            yrr_arr.append([pri_yrr, last_yrr])

        kelly_arr = []
        for tb in tables[6::4]:
            trs = tb.xpath('./tbody/tr')
            pri_kelly = trs[0].xpath('./td/text()')
            last_kelly = trs[1].xpath('./td/text()')
            kelly_arr.append([pri_kelly, last_kelly])

        all_company_odds = []
        for c, odds, chance, yrr, kelly in zip(company_names, odds_arr, chance_arr, yrr_arr, kelly_arr):
            all_company_odds.append([c.encode(GetEachMatchOdds.defaultEncoding),odds[0][0], odds[0][1], odds[0][2], \
            odds[1][0], odds[1][1], odds[1][2], chance[0][0], chance[0][1], chance[0][2], chance[1][0], chance[1][1],\
            chance[1][2], yrr[0][0], yrr[1][0], kelly[0][0], kelly[0][1], kelly[0][2], kelly[1][0], kelly[1][1], kelly[1][2]])
        return all_company_odds

    def parse_euro_old_html(self, response):
        _html = html.fromstring(response.text)

        trs = _html.xpath('//table[@id="datatb"]/tr')

        all_company_odds = []
        
        if(len(_html.xpath('//table[@id="datatb"]')) == 0):
            raise GetHtmlFailed("Can't get Euro odds")
        
        if(len(trs) < 4):
            return []

        for elem, next_elem in zip(trs[2::2], trs[3::2]+[trs[2]]):
            elem_tds = elem.xpath('./td')
            next_elem_tds = next_elem.xpath('./td')
            if len(elem_tds) > 12:
                if(len(elem_tds[1].xpath('./a/text()')) > 0):
                    company = elem_tds[1].xpath('./a/text()')[0].encode(GetEachMatchOdds.defaultEncoding)
                else:
                    company = elem_tds[1].text.encode(GetEachMatchOdds.defaultEncoding)
                all_company_odds.append([company,  elem_tds[2].text, elem_tds[3].text, elem_tds[4].text, next_elem_tds[0].text, \
                next_elem_tds[1].text, next_elem_tds[2].text, elem_tds[5].text, elem_tds[6].text, elem_tds[7].text, next_elem_tds[3].text, \
                next_elem_tds[4].text, next_elem_tds[5].text, elem_tds[8].text, next_elem_tds[6].text, elem_tds[9].xpath('./span/text()')[0], \
                elem_tds[10].xpath('./span/text()')[0], elem_tds[11].text, next_elem_tds[7].xpath('./span/text()')[0], \
                next_elem_tds[8].xpath('./span/text()')[0], next_elem_tds[9].text])
        return all_company_odds

    def parse_asia_new_html(self, response):
        _html = html.fromstring(response.text)

        company_names = _html.xpath('//span[@class="quancheng"]/text()')
        
        if(len(_html.xpath('//table[@class="pl_table_data"]')) == 0):
            raise GetHtmlFailed("Can't get Asia odds")
        
        if(len(company_names) < 1):
            return []

        tables = _html.xpath('//table[@class="pl_table_data"]')
        
        now_odds_arr = []
        pri_odds_arr = []
        for tb1, tb2 in zip(tables[2::2], tables[3::2]):
            tb1_tds = tb1.xpath('./tbody/tr/td/text()')
            tb2_tds = tb2.xpath('./tbody/tr/td/text()')
            now_odds_arr.append(tb1_tds[0:3])
            pri_odds_arr.append(tb2_tds[0:3])

        all_company_odds = []
        for c, pri_odds, now_odds in zip(company_names, pri_odds_arr, now_odds_arr):
            if(len(pri_odds) < 3) or (len(now_odds) < 3):
                continue
            all_company_odds.append([c.encode(GetEachMatchOdds.defaultEncoding), pri_odds[0].encode(GetEachMatchOdds.defaultEncoding), \
            pri_odds[1].replace(u' \xa0', '').encode(GetEachMatchOdds.defaultEncoding), pri_odds[2].encode(GetEachMatchOdds.defaultEncoding), \
            now_odds[0].replace(u'\u2193', '').replace(u'\u2191', '').encode(GetEachMatchOdds.defaultEncoding), \
            now_odds[1].replace(u' \xa0', '').encode(GetEachMatchOdds.defaultEncoding), \
            now_odds[2].replace(u'\u2193', '').replace(u'\u2191', '').encode(GetEachMatchOdds.defaultEncoding)])

        return all_company_odds

    def parse_asia_old_html(self, response):
        _html = html.fromstring(response.text)
        
        trs = _html.xpath('//table[@id="datatb"]//tr')
        
        if(len(_html.xpath('//table[@id="datatb"]')) == 0):
            raise GetHtmlFailed("Can't get Asia odds")
        
        all_company_odds = []
        number_re = re.compile(r'\d\.\d')
        for tr in trs[1:-3]:
            tds = tr.xpath('./td')
            if(len(tds) > 8):
                pri_home_odds = tds[6].text
                pri_away_odds = tds[8].text
                
                if len(tds[2].xpath('./span/text()')) < 1 or len(tds[4].xpath('./span/text()')) < 1:
                	continue
                
                new_home_odds = tds[2].xpath('./span/text()')[0]
                new_away_odds = tds[4].xpath('./span/text()')[0]
                
                if not number_re.search(pri_home_odds) or not number_re.search(pri_away_odds) or not number_re.search(new_home_odds) or \
                        not number_re.search(new_away_odds) or not tds[1].text or not tds[3].text or not tds[7].text:
                            continue

                all_company_odds.append([tds[1].text.replace(u'\ufffd', '').encode(GetEachMatchOdds.defaultEncoding), \
                pri_home_odds, tds[7].text.replace(u' \xa0', '').encode(GetEachMatchOdds.defaultEncoding), \
                pri_away_odds, new_home_odds, tds[3].text.replace(u' \xa0', '').encode(GetEachMatchOdds.defaultEncoding), new_away_odds])
                
        return all_company_odds

if __name__ == '__main__':
    g = GetEachMatchOdds('product', 'db.conf', 'test')
    g.basic_work(2, 0, 150)
