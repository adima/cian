import datetime
import logging
import multiprocessing as mp

import numpy as np
import pandas as pd
import requests
import time

from bs4 import BeautifulSoup
from Parser import parse_row
from Reference import districts

logger = logging.getLogger(__name__)
fh = logging.handlers.RotatingFileHandler('cian.log', maxBytes=50e6, backupCount=3)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s' )


def process_district(driver, district, name):
    def load_page(url, max_errors):
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
        logger.info('Processing %s' % name)
        init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                   '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p=1&totime=86400'.format(district)
        cook = {'serp_view_mode': 'table'}
        soup = load_page(init_url)
        result_n = int(soup.find('div', class_='serp-above__count').strong.getText())
        logger.info('%s %s results' % (district, result_n))
        n_pages = np.ceil(result_n / float(25))
        for page_n in range(1, int(n_pages) + 1):
            init_url = 'http://www.cian.ru/cat.php?deal_type=sale' \
                       '&district%5B0%5D={}&engine_version=2&maxtarea=60&offer_type=flat&p={}&totime=86400'.format(
                district,
                page_n)
            logger.info('District %s: %s. Page %s out of %s' % (district, name,  page_n, n_pages))
            if page_n > 1:
                soup = load_page(init_url)

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

