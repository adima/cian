# coding=utf-8
import selenium.webdriver
from transliterate import translit
import datetime
import itertools
import pandas as pd
import numpy as np
import sys
import multiprocessing as mp
import logging
import logging.handlers
import time

from Reference import districts

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.handlers.RotatingFileHandler('cian.log', maxBytes=50e6, backupCount=3)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s' )
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)


def parse_row(row):
    row_dict = {}
    row_dict['offer_id'] = row.get_attribute('oid')
    # print row_dict['offer_id']
    row_dict['datetime_add'] = datetime.datetime.now()
    columns = ['objects_item_info_col_%s' % n for n in range(1, 10)]
    columns.remove('objects_item_info_col_8')
    for col in columns:
        col_el = row.find_element_by_class_name(col)
        if col == 'objects_item_info_col_1':
            col_divs = col_el.find_elements_by_tag_name('div')
            col_as = col_el.find_elements_by_tag_name('a')
            row_dict['lat'], row_dict['lng'] = \
                col_divs[0].find_element_by_tag_name('input').get_attribute('value').split(',')
            try:
                row_dict['metro_name'] = col_divs[1].find_element_by_tag_name('a').text
                row_dict['metro_distance_lab'] = col_divs[1].find_elements_by_tag_name('span')[1].text
            except:
                pass
            row_dict['city'] = col_as[1].text
            row_dict['district'] = col_as[2].text
            row_dict['address'] = ' '.join(map(lambda x: x.text, col_as[3:]))
        elif col[-1] == '2':
            row_dict['room_number_lab'] = col_el.find_element_by_tag_name('a').text
        elif col[-1] == '3':
            for col_it in col_el.text.split('\n'):
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
            for n, it in enumerate(col_el.text.split('\n')):
                try:
                    number = int(''.join([el for el in it if el.isdigit()]))
                except:
                    continue
                if n == 0:
                    row_dict['price_rub'] = number
                elif n == 1:
                    row_dict['price_dollars'] = number
                elif n == 2:
                    row_dict['price_square_meter_r'] = number
        elif col[-1] == '5':
            split = col_el.text.split('\n')
            row_dict['floor_raw'] = split[0]
            if len(split) > 1:
                row_dict['house_type'] = split[1]
        elif col[-1] == '6':
            for item in col_el.text.split('\n'):
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
            row_dict['telephone'] = col_el.text

        elif col[-1] == '9':
            split = col_el.text.split('\n')
            len_split = len(split)
            for n, it in enumerate(split):
                if n == 0:
                    row_dict['pay_status'] = it
                    continue
                elif n == 1:
                    row_dict['date_raw'] = it
                    continue
                elif n == 2:
                    row_dict['seller_id'] = it
                    continue
                if len_split == 7:
                    if n == 3:
                        row_dict['seller_status'] = it
                    elif n == 4:
                        row_dict['ad_text'] = it
                else:
                    if n == 3:
                        row_dict['ad_text'] = it
            for el in col_el.find_elements_by_tag_name('a'):
                if el.text == u'Перейти к странице объявления':
                    row_dict['ad_href'] = el.get_attribute('href')
    return row_dict


def process_district(driver, district, name):
    try:
        final_list = []
        print 'Processing %s' % name
        init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                   '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p=1&totime=86400'.format(district)
        page_loaded = False
        while not page_loaded:
            try:
                driver.get(init_url)
                page_loaded = True
            except:
                logger.exception('Loading page exception')
                page_loaded = False

        [pp.click() for pp in driver.find_elements_by_class_name("popup_closer") if pp.is_displayed()]
        # if district == districts.index[0]:
        #     driver.find_element_by_xpath('//*[@id="layout"]/div[3]/div/div[2]/div/div[2]/a[1]').click()
        result_n = int(
            driver.find_element_by_class_name('serp-above__count').find_elements_by_tag_name('strong')[0].text)
        print '%s results' % result_n
        n_pages = np.ceil(result_n / float(25))
        for page_n in range(1, int(n_pages) + 1):
            init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                       '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p={}&totime=86400'.format(
                district,
                page_n)
            logger.info('District %s: %s. Page %s out of %s' % (district, name,  page_n, n_pages))
            if page_n > 1:
                page_loaded = False
                error_ct = 0
                while not page_loaded:
                    try:
                        driver.get(init_url)
                        page_loaded = True
                    except:
                        logger.exception('Loading page exception')
                        page_loaded = False
                        error_ct += 1
                        if error_ct == 10:
                            driver = makePhantomJS()


            driver.save_screenshot('screen.png')
            tbody = driver.find_elements_by_tag_name('tbody')[1]
            if len(tbody.text) == 0:
                page_loaded = False
                while not page_loaded:
                    try:
                        [pp.click() for pp in driver.find_elements_by_class_name("popup_closer") if pp.is_displayed()]
                        driver.find_element_by_xpath('//*[@id="layout"]/div[3]/div/div[2]/div/div[2]/a[1]').click()
                        tbody = driver.find_elements_by_tag_name('tbody')[1]
                        driver.save_screenshot('screen.png')
                        page_loaded = True
                    except:
                        logger.exception('Switch to other view')
                        page_loaded = False

                        # [pp.click() for pp in driver.find_elements_by_class_name("popup_closer") if pp.is_displayed()]
                        # driver.find_element_by_xpath('//*[@id="layout"]/div[3]/div/div[2]/div/div[2]/a[1]').click()
                    # [pp.click() for pp in driver.find_elements_by_class_name("popup_closer") if pp.is_displayed()]

            # for tbody in tbodies:
            rows = tbody.find_elements_by_tag_name('tr')
            rows = [row for row in rows if row.get_attribute('id').split('_')[0] == 'offer']
            res = pd.DataFrame(map(parse_row, rows))
            # res
            final_list.append(res)
        res_final = pd.concat(final_list)
        res_final.to_pickle('res_%s_%s.pickle' % (district, datetime.datetime.now().strftime('%m%d%H%M%s')))
        return True
    except:
        # print ("Unexpected error:", sys.exc_info()[0])
        logger.exception('Exception %s ' % name)
        driver.save_screenshot('error_%s_%s.png' % (name, datetime.datetime.now().strftime('%m%d%H%M%s')))
        return False

def main(districts=districts):
    driver = makePhantomJS()
    for district, name in districts.iteritems():
        main_district(district, name, driver)


def main_district(district, name, driver=None):
    if driver is None:
        driver = makePhantomJS()
    processed = False
    n_errors = 0
    while not processed:
        processed = process_district(driver, district, name)
        if not processed:
            driver = makePhantomJS()
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


def makePhantomJS():
    driver = selenium.webdriver.PhantomJS()
    # driver = selenium.webdriver.Firefox()
    driver.implicitly_wait(30)
    driver.set_page_load_timeout(60)
    return driver
    # return selenium.webdriver.Firefox()

if __name__ == '__main__':
    # driver = makePhantomJS()
    # driver.get('http://www.cian.ru/kupit-kvartiru/')
    # driver.find_element_by_xpath('//*[@id="layout"]/div[3]/div/div[2]/div/div[2]/a[1]').click()
    # driver.save_screenshot('screen.png')
    #
    # tbodies = driver.find_elements_by_tag_name('tbody')[1:]
    # for tbody in tbodies:
    #     rows = tbody.find_elements_by_tag_name('tr')
    #     rows = [row for row in rows if row.get_attribute('id').split('_')[0] == 'offer']
    #     res = map(parse_row, rows)
    #     pass
    # district = None
    # page = None
    #

    # for row in rows:
    #     parse_row(row)
    # main(districts.iloc[120:])
    # main(districts.iloc[120:]) #good district for exception debugging
    mainConc(districts.ix[43:], 1)

