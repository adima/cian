# coding=utf-8
from samba.netcmd.dns import data_to_dns_record

import datetime
import logging
import logging.handlers
import multiprocessing as mp
import os

import numpy as np
import pandas as pd
import requests
import time
import pickle

from bs4 import BeautifulSoup
from transliterate import translit
from Reference import districts
import sys

import airflow

sys.setrecursionlimit(50000)
conn = airflow.hooks.MySqlHook('data').get_conn()


logger = logging.getLogger(__name__)
fh = logging.handlers.RotatingFileHandler('cian.log', maxBytes=50e6, backupCount=3)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s' )

logger.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)

dt_start = datetime.datetime.now()
start_str = datetime.datetime.now().strftime('%Y%m%d%H%M')
pickle_path = os.path.join('.', 'pickle', start_str)
if not os.path.exists(pickle_path):
    os.mkdir(pickle_path)


def parse_row(row):
    row_dict = {}
    row_dict['offer_id'] = row.get('oid')
    # print row_dict['offer_id']
    row_dict['datetime_add'] = datetime.datetime.now()
    columns = ['objects_item_info_col_%s' % n for n in range(1, 10)]
    columns.remove('objects_item_info_col_8')
    for col in columns:
        col_el = row.find('td', class_=col)
        if col == 'objects_item_info_col_1':
            col_divs = col_el.find_all('div')
            col_as = col_el.find_all('a')
            row_dict['lat'], row_dict['lng'] = \
                col_divs[0].find('input').get('value').split(',')
            try:
                row_dict['metro_name'] = col_divs[1].find('a').getText()
                row_dict['metro_distance_lab'] = col_divs[1].find('span')[1].getText()
            except:
                pass
            row_dict['city'] = col_as[1].getText()
            # row_dict['district'] = col_as[2].getText()
            row_dict['address'] = ' '.join(map(lambda x: x.getText(), col_as[3:]))
        elif col[-1] == '2':
            row_dict['room_number_lab'] = col_el.find('a').getText()
        elif col[-1] == '3':
            for col_it in col_el.getText().split('\n'):
                if len(col_it) > 0:
                    col_it_split = col_it.split(':')
                    split_first = col_it_split[0]
                    if split_first == u'Общая':
                        row_dict['area_overall_raw'] = col_it_split[1]
                    elif split_first == u'Кухня':
                        row_dict['area_kitchen_raw'] = col_it_split[1]
                    elif split_first == u'Жилая':
                        row_dict['area_living_raw'] = col_it_split[1]
                    else:
                        row_dict['area_rooms_raw'] = col_it_split[0]
        elif col[-1] == '4':
            get_digit = lambda x: int(''.join([el for el in x if el.isdigit()]))
            row_dict['price_rub'] = get_digit(col_el.find('div', class_='objects_item_price').strong.getText())
            row_dict['price_dollars'] = get_digit(col_el.find('div', class_='objects_item_second_price').getText())
            row_dict['price_square_meter_r'] = get_digit(col_el.find('div', style='color:green;').getText())
        elif col[-1] == '5':
            split = col_el.getText().split('\n')
            split = map(lambda x: x.replace(' ', ''), split)
            split = [x for x in split if len(x) > 0]
            row_dict['floor_raw'] = split[0] if len(split) > 0 else pd.np.nan
            if len(split) > 1:
                row_dict['house_type'] = split[1]
        elif col[-1] == '6':
            tds = col_el.find_all('td')
            tds = map(lambda x: x.get_text(strip=True).replace('\n', '').replace(' ', ''), tds)
            for item in tds:
                item_split = item.split(':')
                if len(item_split) > 1:
                    key = 'addinfo_' + translit(item_split[0], reversed=True).lower().replace(' ', '_')
                    row_dict[key] = ':'.join(item_split[1:])
                else:
                    item_val = item_split[0]
                    if item_val == u'Новостройка':
                        row_dict['novostroi'] = 1
                    elif item_val == u'Вторичка':
                        row_dict['vtorichka'] = 1
                    elif item_val == u'Свободная':
                        row_dict['deal_free'] = 1
                    elif item_val == u'Альтернатива':
                        row_dict['deal_alt'] = 1
        elif col[-1] == '7':
            row_dict['telephone'] = col_el.getText(strip=True)

        elif col[-1] == '9':
            row_dict['pay_status'] =\
                col_el.find('a',
                        class_ = 'c-iconed c-iconed_m objects_item_payment_status_link_paid').get_text(strip=True)
            row_dict['date_raw'] = col_el.find('span', class_='objects_item_dt_added').get_text(strip=True)
            row_dict['seller_id'] = [a for a in col_el.find_all('a') if 'cat.php?id_user' in a.get('href')][0].get_text()
            seller_staus = col_el.find('span', class_='objects_item_realtor_checked_text')
            row_dict['seller_status'] = seller_staus.get_text(strip=True) if seller_staus else pd.np.nan
            row_dict['ad_text'] = col_el.find('div', class_='objects_item_info_col_comment_text no-truncate').get_text(strip=True)
            row_dict['ad_href'] = col_el.find('a', class_='objects_item_info_col_card_link no-mobile').get('href')
    return row_dict



def process_district(driver, district, name):
    def load_page(url, max_errors=10):
        errors = 0
        while errors <= max_errors:
            try:
                resp = requests.get(url, cookies=cook)
                return BeautifulSoup(resp.text, 'html.parser')
            except:
                logger.exception('Loading page exception')
                errors += 1
                time.sleep(5)
    try:
        final_list = []
        logger.info(u'Processing %s' % name.decode('utf-8'))
        init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                   '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p=1&totime=86400'.format(district)
        cook = {'serp_view_mode': 'table'}
        soup = load_page(init_url)
        try:
            result_n = int(soup.find('div', class_='serp-above__count').strong.getText())
        except AttributeError:
            return True
        logger.info('%s %s results' % (district, result_n))
        n_pages = np.ceil(result_n / float(25))
        for page_n in range(1, int(n_pages) + 1):
            init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                       '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p={}&totime=86400'.format(
                district,
                page_n)
            logger.info(u'District %s: %s. Page %s out of %s' % (district, name.decode('utf-8'),  page_n, n_pages))
            if page_n > 1:
                soup = load_page(init_url)

            rows = [row for row in soup.body.find_all('table')[1].tbody.find_all('tr') if row.get('id')]
            res = pd.DataFrame(map(parse_row, rows))
            # res
            final_list.append(res)
        res_final = pd.concat(final_list)
        res_final['district'] = name
        res_final['datetime_add'] = dt_start
        res_final.to_sql('raw_cian', conn, if_exists='append', flavor='mysql', index=False)
        res_final.to_pickle('%s/res_%s_%s.pickle' % (pickle_path, district, datetime.datetime.now().strftime('%m%d%H%M%s')))
        return True
    except:
        # print ("Unexpected error:", sys.exc_info()[0])
        logger.exception(u'Exception %s, %s ' % (name.decode('utf-8'), init_url))
        if 'soup' in locals():
            with open('./error/exception_soup_%s' % datetime.datetime.now().strftime('%m%d%H%M%s'), 'w') as f:
                pickle.dump(soup, f)
        return False



def main_district(district, name, driver=None):
    processed = False
    n_errors = 0
    while not processed:
        processed = process_district(driver, district, name)
        if not processed:
            n_errors += 1
            if n_errors > 5:
                break


def main_district_cc(q_in, i):
    print 'Starting thread # %s' % (i + 1)
    while not q_in.empty():
        district, name = q_in.get()
        # driver = makePhantomJS()
        main_district(district, name)


def mainConc(districts, n_threads):
    manager = mp.Manager()
    q_in = manager.Queue()
    [q_in.put((district, name)) for district, name in districts.iteritems()]
    processes = [mp.Process(target=main_district_cc, args=(q_in, i))
                             for i in range(n_threads)]
    [p.start() for p in processes]
    [p.join() for p in processes]


if __name__ == '__main__':
    mainConc(districts, 10)