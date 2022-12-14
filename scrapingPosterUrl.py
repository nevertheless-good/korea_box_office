from selenium import webdriver
import time
import warnings

warnings.filterwarnings(action='ignore')

def getPosterURL(movieCd):
	driver = webdriver.Chrome('./chromedriver')

	# driver.implicitly_wait(3)

	driver.get(f'https://www.kobis.or.kr/kobis/business/mast/mvie/searchUserMovCdList.do?movieCd={movieCd}')

	main_window = driver.current_window_handle

	driver.find_element("xpath", '/html/body/div/div[2]/div[2]/div[4]/table/tbody/tr[1]/td[1]/a').click()

	time.sleep(1)

	for handle in driver.window_handles: 
	    if handle != main_window: 
	        popup = handle
	        driver.switch_to_window(popup)
	        find_element = True


	elements = driver.find_elements("xpath", '/html/body/div[2]/div[2]/div/div[1]/div[2]/a/img')

	print(elements[0].get_attribute("src"))

	driver.close()


getPosterURL('20210001')
getPosterURL('20211792')

