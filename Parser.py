# coding=utf-8
import datetime
import logging
import logging.handlers



from Main import logger, fh, ch, formatter, mainConc
from Reference import districts
from bs4 import BeautifulSoup



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

