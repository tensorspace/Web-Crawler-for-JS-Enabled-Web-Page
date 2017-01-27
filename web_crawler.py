#!/usr/bin/env python
# -*- coding: utf-8 -*-
# from bs4 import BeautifulSoup
from selenium import  webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time, datetime, sys, re

def wait(driver, max_time, step, dt):
    flag = False
    for t in range(1, int(max_time / step) + 1):
        status = driver.execute_script('return document.readyState')
        time.sleep(step)
        if status[0] != 'c':
            flag = True
        if flag and status[0] == 'c':
            return True
    return False

def site_date(dt):
    driver = webdriver.PhantomJS()
    while True:
        driver.get('http://spds.qhrb.com.cn/SP10/SPOverSee1.aspx')  # link a headless webdriver to the website
        time.sleep(10)
        print('connected')
        elem = driver.find_element_by_name('txtTradeDate')
        elem.clear()
        elem.send_keys(str(dt))
        elem.send_keys(Keys.RETURN) # search for result of a given date
        status = wait(driver, 30, 0.01, dt)
        if status:
            break
        else:
            print(str(dt) + ': try reconnect to the website')
    return driver

def category(driver, i, dt):
    #category = driver.find_element_by_id('lbAccountType' + str(i)).text  # the category of ranking
    while True:
        driver.execute_script('WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(\"lbAccountType' + str(
            i) + '\", \"\", true, \"\", \"\", false, true))')
        status = wait(driver, 30, 0.01, dt)
        if status:
            break
        else:
            print(str(dt) + ': try reload the category')
            driver = site_date(dt)
    options = driver.find_elements_by_tag_name('option')
    total_page = int(options[-1].text) # determine the total number of page from the last item in the option menu
    return total_page

def page(driver, i, j, dt):
    while True:
        driver.execute_script('__doPostBack(\'AspNetPager1\',\'' + str(j) + '\')')
        status = wait(driver, 30, 0.01, dt)
        if status:
            break
        else:
            print(str(dt) + ': try reload the page')
            driver = site_date(dt)
            category(driver,i, dt)
    elem = driver.find_elements_by_class_name('admintable')
    table = elem[0].text
    first_eol = table.find('\n')
    content = re.sub(r' ', r',', table[first_eol + 1:] + '\n')
    return content

def main():
    dt = sys.argv[1].split('-') # start date
    dt = datetime.date(int(dt[0]), int(dt[1]), int(dt[2]))
    print(sys.argv[1])
    csv_file = open(str(dt) + '.csv', 'w+')
    start_time = time.time()
    driver = site_date(dt)
    for i in range(1, 12): # for each catogory of ranking
        total_page = category(driver,i, dt)
        for j in range(1, total_page + 1): # for each page of ranking
            current_time = time.time()
            if current_time - start_time > 900:
                time.sleep(60)
                start_time = current_time
            content = page(driver, j, dt)
            csv_file.write(content)

if __name__ == '__main__':
    main()
