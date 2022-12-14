from decouple import config
from datetime import date, timedelta 
import json
import requests
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
import warnings

warnings.filterwarnings(action='ignore')


KOFIC_API_KEY = config('KOFIC_API_KEY')
MARIADB_HOST = config('MARIADB_HOST')
MARIADB_PORT = config('MARIADB_PORT')
MARIADB_ID = config('MARIADB_ID')
MARIADB_PW = config('MARIADB_PW')

BASE_URL = f'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={KOFIC_API_KEY}&targetDt='

# Connection of MariaDB
def connectMariaDB():
	conn  = pymysql.connect(host=MARIADB_HOST,
	                                port=int(MARIADB_PORT),
	                                user=MARIADB_ID,
	                                password=MARIADB_PW,
	                                db='movieInfoDB',
	                                charset='utf8')
	cur = conn.cursor()

	return conn, cur

def closeMariaDB(conn, cur):
	cur.close()
	conn.close()

# Create weeklyBoxOffie Table
def createWeeklyBoxOffice2MariaDB(conn, cur):

	cur.execute('DROP TABLE IF EXISTS weeklyBoxOffice')
	conn.commit()

	sql = '''CREATE TABLE weeklyBoxOffice (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				date VARCHAR(20) NOT NULL,
				rankInten VARCHAR(10),
				movieNm VARCHAR(255),
				openDt VARCHAR(20),
				audiAcc VARCHAR(30)
				)
			'''
	cur.execute(sql)
	conn.commit()


# Insert Data to DB
def insertData2MariaDB(conn, cur, data):
	sql = 'INSERT INTO weeklyBoxOffice (date, rankInten, movieNm, openDt, audiAcc) VALUES (%s, %s, %s, %s, %s);'
	cur.execute(sql, data)
	conn.commit()


def updateWeeklyBoxOffic():
	print("Run updateWeeklyBoxOffic")

	conn, cur = connectMariaDB()

	createWeeklyBoxOffice2MariaDB(conn, cur)

	headers = {
		'Content-Type': 'application/json',
		'Connection': 'close',
	}

	for diff in range(0, 10):

		dateStr = (date.today() - timedelta(diff)).strftime("%Y%m%d")
		request_url = BASE_URL + str(dateStr)

		with requests.session() as s:
			s.keep_alive = False

			try:
				response = s.get(request_url, headers=headers)
				
				json_data = response.json()

				if len(json_data['boxOfficeResult']['weeklyBoxOfficeList']) == 0:
					continue

				date_str = json_data['boxOfficeResult']['showRange']
		
				for idx in range(0, 10):
					rankInten = json_data['boxOfficeResult']['weeklyBoxOfficeList'][idx]['rankInten']
					movieNm = json_data['boxOfficeResult']['weeklyBoxOfficeList'][idx]['movieNm']
					openDt = json_data['boxOfficeResult']['weeklyBoxOfficeList'][idx]['openDt']
					audiAcc = json_data['boxOfficeResult']['weeklyBoxOfficeList'][idx]['audiAcc']

					insertData2MariaDB(conn, cur, [date_str, rankInten, movieNm, openDt, audiAcc])

				break

			except Exception as e:
					print(repr(e))
			finally:
				s.close()

	closeMariaDB(conn, cur)


updateWeeklyBoxOffic()

sched = BlockingScheduler()
sched.add_job(updateWeeklyBoxOffic, 'cron', hour='05', minute='30', id='weeklyMorning')
sched.add_job(updateWeeklyBoxOffic, 'cron', hour='18', minute='00', id='weeklyEvening')
sched.start()
