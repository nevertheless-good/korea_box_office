from decouple import config
import pandas as pd
import pymysql
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pickle
import warnings
import time

start_time = time.time()

warnings.filterwarnings(action='ignore')

MARIADB_HOST = config('MARIADB_HOST')
MARIADB_PORT = config('MARIADB_PORT')
MARIADB_ID = config('MARIADB_ID')
MARIADB_PW = config('MARIADB_PW')

# Connection of MariaDB
maria_conn  = pymysql.connect(host=MARIADB_HOST,
                                port=int(MARIADB_PORT),
                                user=MARIADB_ID,
                                password=MARIADB_PW,
                                db='movieInfoDB',
                                charset='utf8')

df = pd.read_sql_query('SELECT * FROM movieInfo', maria_conn)

# df = df[df['movieCd'] > '20200000']
# df.reset_index(inplace=True)

print(df.shape)

indices = pd.Series(df.index, index=df['movieName'])

count = CountVectorizer()


count_matrix = count.fit_transform(df['cossim'])
cosin_sim = cosine_similarity(count_matrix, count_matrix)

def getRecomendTop10(title):
   idx = indices[title]
   sim_scores = list(enumerate(cosin_sim[idx]))
   sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
   movie_indices = [i[0] for i in sim_scores[1:11]]
   return df['movieName'].iloc[movie_indices]


reco = getRecomendTop10('기묘한 이야기')

movies = df[['id', 'movieCd', 'movieName', 'posterURL']].copy()
pickle.dump(movies, open('movies.pickle', 'wb'))
pickle.dump(cosin_sim, open('cosine_sim.pickle', 'wb'))

maria_conn.close()

end_time = time.time()

print(f"{end_time - start_time:.5f} sec")
