import pickle
import streamlit as st
from decouple import config
import pymysql
import time



MARIADB_HOST = config('MARIADB_HOST')
MARIADB_PORT = config('MARIADB_PORT')
MARIADB_ID = config('MARIADB_ID')
MARIADB_PW = config('MARIADB_PW')


movies = pickle.load(open('movies.pickle', 'rb'))
cosine_sim = pickle.load(open('cosine_sim.pickle', 'rb'))

def get_recommenations(title):
	index = movies[movies['movieName'] == title].index[0]
	sim_scores = list(enumerate(cosine_sim[index]))
	sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
	sim_scores = sim_scores[1:11]
	movie_indices = [i[0] for i in sim_scores]
	titles = []
	images = []
	for i in movie_indices:
		id = movies['id'].iloc[i]
		# titles.append(movies['movieName'].iloc[i])
		title_link = f"""[{movies['movieName'].iloc[i]}](https://www.kobis.or.kr/kobis/business/mast/mvie/searchUserMovCdList.do?movieCd={movies['movieCd'].iloc[i]})
				"""
		titles.append(title_link)

		images.append(movies['posterURL'].iloc[i])

	return titles, images

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

# Create recomendHistory Table
def createRecomendHistory2MariaDB(conn, cur):

	check_sql = "SHOW TABLES LIKE 'recomendHistory'"
	cur.execute(check_sql)
	result = cur.fetchall()

	if result:
		return

	sql = '''CREATE TABLE recomendHistory (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				date VARCHAR(20) NOT NULL,
				title VARCHAR(255)
				)
			'''
	cur.execute(sql)
	conn.commit()


# Insert Data to DB
def insertData2MariaDB(conn, cur, data):
	sql = 'INSERT INTO recomendHistory (date, title) VALUES (%s, %s);'
	cur.execute(sql, data)
	conn.commit()



def saveRecomendHistory(title):
	conn, cur = connectMariaDB()

	createRecomendHistory2MariaDB(conn, cur)

	date_str = time.strftime('%Y.%m.%d %H:%M:%S')

	insertData2MariaDB(conn, cur, [date_str, title])

	closeMariaDB(conn, cur)




st.set_page_config(layout='wide')
st.header('영화추천서비스')

movie_list = movies['movieName'].values
title = st.selectbox('선택한 영화의 장르, 감독, 배우의 정보를 바탕으로 영화를 추천합니다.', movie_list)
if st.button('Recomend'):
	titles, images = get_recommenations(title)

	saveRecomendHistory(title)

	idx = 0
	for i in range(0, 2):
		cols = st.columns(5)
		for col in cols:
			col.write(titles[idx])
			col.image(images[idx], width = 200)
			idx += 1
