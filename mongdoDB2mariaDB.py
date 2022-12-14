from decouple import config
import time
import json
import pymongo
import pymysql
from selenium import webdriver
import warnings

warnings.filterwarnings(action='ignore')

# Information of Config
MONGODB_ID = config('MONGODB_ID')
MONGODB_PW = config('MONGODB_PW')
MARIADB_HOST = config('MARIADB_HOST')
MARIADB_PORT = config('MARIADB_PORT')
MARIADB_ID = config('MARIADB_ID')
MARIADB_PW = config('MARIADB_PW')

# Connection of MongoDB
mongo_client = pymongo.MongoClient(f"mongodb://{MONGODB_ID}:{MONGODB_PW}@192.168.15.2:27017/")
mongo_database = mongo_client.movie_db
mongo_collection = mongo_database.movie_info

# Connection of MariaDB
maria_conn  = pymysql.connect(host=MARIADB_HOST, 
								port=int(MARIADB_PORT), 
								user=MARIADB_ID, 
								password=MARIADB_PW,
								db='movieInfoDB',
								charset='utf8')
maria_cur = maria_conn.cursor()

# Get Number of Collections
def getSizeOfCollection():
	return mongo_collection.estimated_document_count()

# Get Document of Index
def getDocumentFromMongoDB(index):
	return mongo_collection.find()[index]

# Create DB on MariaDB
def createDB2MariaDB(dbName):
	maria_cur.execute(f"CREATE DATABASE {dbName}")

# Create movieInfo Table
def createMovieInfo2MariaDB():

	check_sql = "SHOW TABLES LIKE 'movieInfo'"
	maria_cur.execute(check_sql)
	result = maria_cur.fetchall()

	if result:
		return


#	sql = '''CREATE TABLE movieInfo (
#        	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
#			movieCd INT NOT NULL,
#        	movieName VARCHAR(255) NOT NULL,
#			movieNameOrg VARCHAR(255),
#			genre VARCHAR(20),
#        	director VARCHAR(255),
#			actor VARCHAR(255),
#			cossim VARCHAR(511)
#       	 )
#         '''

	sql = '''CREATE TABLE movieInfo (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				movieCd VARCHAR(10) NOT NULL,
				movieName VARCHAR(255),
				genre VARCHAR(20),
				cossim VARCHAR(511),
				posterURL VARCHAR(511)
			)
			'''


	maria_cur.execute(sql)

# Insert Data to DB
def insertData2MariaDB(data):
	sql = 'INSERT INTO movieInfo (movieCd, movieName, genre, cossim, posterURL) VALUES (%s, %s, %s, %s, %s);'
	maria_cur.execute(sql, data)
	maria_conn.commit() 

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

	return elements[0].get_attribute("src")

	driver.close()


#getDocumentFromMongoDB(0)
movie_info_count  = getSizeOfCollection()
#movie_info_count = 2


# Create DB on MariaDB
# createDB2MariaDB("movieInfo")

# Create MovieInfo Table
createMovieInfo2MariaDB()

empty_genre_count = 0

for idx in range(0, movie_info_count):

	if (idx+1) % 100 == 0:
		print(f"processing {idx+1}....")

	document = getDocumentFromMongoDB(idx)

	# Json Parsing
	movieCd = document['movieCd']
	movieName = document['movieNm']
	genres_sim = ' '.join({v.lower().replace(" ", "") for d in document['genres'][:3] for k, v in d.items()})
	directors = ' '.join({v.lower().replace(" ", "") for d in document['directors'][:3] for k, v in d.items() if k == "peopleNm"})
	actors = ' '.join({v.lower().replace(" ", "") for d in document['actors'][:3] for k, v in d.items() if k == "peopleNm"})

	genres = genres_sim.split(' ')[0]
	#cossim = movieName.lower().replace(" ", "") + ' ' + genres_sim + ' ' + directors + ' ' + actors
	cossim = genres_sim + ' ' + directors + ' ' + actors

	if genres == '' :
		empty_genre_count += 1
		if (empty_genre_count % 100) == 0:
			print(f"Genre Empty: {empty_genre_count}")
		continue

	posterUrl = getPosterURL(movieCd)

	insertData2MariaDB([movieCd, movieName, genres, cossim, posterUrl])
 

# Close MongoDB
mongo_client.close()

# Close Connection of MariaDB
maria_cur.close()
maria_conn.close()

