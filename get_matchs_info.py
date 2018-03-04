# -*- coding: utf-8 -*-

import db_connecter
import io
import logging
import requests
import re
import sys

from datetime import datetime, timedelta
from lxml import html
from logging import config
from random import randint
from time import sleep

class GetMatchsInfo:
    defaultEncoding = 'utf-8'
    logLevel = 'product'
    baseUrl = 'http://live.500.com/?e='

    def __init__(self):
        logging.config.fileConfig('logger_test.conf')
        self.logger = logging.getLogger(GetMatchsInfo.logLevel)
        self.db_conn = db_connecter.DBConnecter('db.conf', 'test')
        self.db_conn.connect()
        pass

    def get_matchs_info(self, date_start_str, date_end_str):
        date_current = datetime.strptime(date_start_str, '%Y-%m-%d')
        date_end = datetime.strptime(date_end_str, '%Y-%m-%d')
        headers = {'User-Agent':'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)', 'Referer':'https://developer.mozilla.org/en-US/docs/Web/JavaScript'}
        while(date_current <= date_end):
            date_current_str = date_current.strftime('%Y-%m-%d')
            url = GetMatchsInfo.baseUrl + date_current_str
            self.logger.info(url)
            #proxies = {'http':'http://124.234.157.29'}
            #r = requests.get(url, headers=headers, proxies=proxies)
            r = requests.get(url, headers=headers)
            r.encoding = r.apparent_encoding
            
            #f = open('data/'+date_current.strftime('%Y-%m-%d')+'.txt', 'w')
            #f.write(r.text.encode(GetMatchsInfo.defaultEncoding))
            #f.close
            
            matchs_info = self.parse_match_list_html(r, date_current_str)
            date_current = date_current + timedelta(days=1)
            sql_str = '''insert into matchs
            (type, 
            type_url, 
            league_turn, 
            start_time, 
            status, 
            home_team,
            home_team_url, 
            home_team_score, 
            concede_point,
            away_team_score,
            away_team,
            away_team_url,
            half_score,
            result,
            analysis_url,
            asia_data_url,
            euro_data_url
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
            %s
            )
            '''
            for match in matchs_info:
                self.db_conn.insert_record(sql_str, tuple(match))
                #self.logger.info(str(match))
            self.db_conn.commit()
            sleep(randint(1, 5))

        self.db_conn.close()
        pass

    def parse_match_list_html(self, response, date_current_str):
        _html = html.fromstring(response.text)
        trs = _html.xpath('//table[@id="table_match"]//tr')
        matchs = []
        for tr in trs[1:]:
            tds = tr.xpath('.//td')
            if(len(tds) < 13):
                continue

            if(tds[2].text is None):
                continue

            _match = []
            match_type = tds[1].xpath('./a/text()')[0]
            _match.append(match_type.encode(GetMatchsInfo.defaultEncoding))
            match_type_url = tds[1].xpath('./a/@href')[0]
            _match.append(match_type_url)
            league_turn = tds[2].text
            _match.append(league_turn.encode(GetMatchsInfo.defaultEncoding))
            start_time = re.sub(r'\d\d-\d\d', date_current_str, tds[3].text)
            _match.append(start_time)
            
            if(len(tds[4].xpath('./span/text()')) > 0):
                status = tds[4].xpath('./span/text()')[0]
            else:
                status = tds[4].text
            
            _match.append(status.encode(GetMatchsInfo.defaultEncoding))
            home_team = tds[5].xpath('./a/text()')[0]
            _match.append(home_team.encode(GetMatchsInfo.defaultEncoding))
            home_team_url = tds[5].xpath('./a/@href')[0]
            _match.append(home_team_url)

            if(len(tds[6].xpath('./div/a/text()')) > 2):
                score_info = tds[6].xpath('./div/a/text()')
            else:
                score_info = ['0', u'\u5e73\u624b', '0']
            
            home_team_score = score_info[0]
            _match.append(home_team_score)
            concede_point = score_info[1]
            _match.append(concede_point.encode(GetMatchsInfo.defaultEncoding))
            away_team_score = score_info[2]
            _match.append(away_team_score)
            away_team = tds[7].xpath('./a/text()')[0]
            _match.append(away_team.encode(GetMatchsInfo.defaultEncoding))
            away_team_url = tds[7].xpath('./a/@href')[0]
            _match.append(away_team_url)
            half_score = tds[8].text
            _match.append(half_score)
            
            if(len(tds[11].xpath('./a/@href'))>2):
                result = tds[10].text
                all_analysis_data = tds[11].xpath('./a/@href')
            else:
                all_analysis_data = tds[10].xpath('./a/@href')
                result = tds[11].text
            
            _match.append(result.encode(GetMatchsInfo.defaultEncoding))
            analysis_url = all_analysis_data[0]
            _match.append(analysis_url)
            asia_data_url = all_analysis_data[1]
            _match.append(asia_data_url)
            euro_data_url = all_analysis_data[2]
            _match.append(euro_data_url)
            matchs.append(_match)
        return matchs

if __name__ == '__main__':
    get_m = GetMatchsInfo()
    get_m.get_matchs_info('2018-01-13', '2018-01-23')
