# coding=utf-8
import datetime
import logging
import logging.handlers

from transliterate import translit

from Main import logger, fh, ch, formatter, mainConc
from Reference import districts
from bs4 import BeautifulSoup

logger.setLevel(logging.DEBUG)
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
            row_dict['district'] = col_as[2].getText()
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
            for n, it in enumerate(col_el.getText().split('\n')):
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

