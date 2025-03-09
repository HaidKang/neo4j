# pip install neo4j-genai
# pip install neo4j
# pip install neo4j-driver
# pip install openai
# pip install neo4j-graphrag

import os
import time
from neo4j import GraphDatabase
import openai
from neo4j_graphrag.retrievers import VectorRetriever
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings


# 환경 변수 설정
from dotenv import load_dotenv
load_dotenv()
os.environ['OPENAI_API_KEY'] = os.getenv("OPENAI_API_KEY") 

# Neo4j 연결 정보
URI = "neo4j://10.103.8.137:7687"  # 보안 연결 시 "neo4j+s://" 사용
USERNAME = "neo4j"
PASSWORD = "neo4j.123456"
DATABASE = "testkkm"

# Neo4j 연결 클래스 정의
class Neo4jConnection:
    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
    
    def close(self):
        """Neo4j 연결 종료"""
        self.driver.close()
    
    def query(self, cypher_query, parameters=None, fetch=True):
        with self.driver.session(database=self.database) as session:
            result = session.run(cypher_query, parameters or {})
            return [record for record in result] if fetch else None

# 연결 초기화
conn = Neo4jConnection(URI, USERNAME, PASSWORD, DATABASE)

# # 영화 추천 Cypher 쿼리
# cypher_query = '''
# MATCH (m:Movie {title:$movie})<-[:RATED]-(u:User)-[:RATED]->(rec:Movie)
# RETURN distinct rec.title AS recommendation LIMIT 20
# '''

# # 쿼리 실행
# results = conn.query(cypher_query, {"movie": "Crimson Tide"})

# # 결과 출력
# for record in results:
#     print(record['recommendation'])

# 벡터 인덱스 생성 함수
def create_vector_index():
    with conn.driver.session(database=conn.database) as session:
        # 기존 인덱스 삭제 (있을 경우)
        session.run("DROP INDEX moviePlotsEmbedding IF EXISTS")

        # 새로운 벡터 인덱스 생성
        session.run(
            """
            CREATE VECTOR INDEX moviePlotsEmbedding IF NOT EXISTS 
            FOR (m:Movie)
            ON (m.plotEmbedding)
            OPTIONS { indexConfig: { `vector.dimensions`: 1536, `vector.similarity_function`: 'cosine' } }
            """
        )
        print("Vector index 'moviePlotsEmbedding' created successfully.")

# 인덱스 존재 여부 확인 함수
def check_index_exists():
    with conn.driver.session(database=conn.database) as session:
        result = session.run("SHOW INDEXES")
        for record in result:
            if "moviePlotsEmbedding" in record.values():
                print("Index is available.")
                return True
    return False

# 인덱스가 적용될 때까지 대기하는 함수
def wait_for_index(max_wait_time=10):
    print("Waiting for index to be applied...")
    for _ in range(max_wait_time):
        if check_index_exists():
            print("Index is now available.")
            return
        time.sleep(1)  # 1초 대기
    raise Exception("Index creation timed out.")

# 벡터 인덱스 생성 실행
create_vector_index()

# 인덱스가 적용될 때까지 대기
wait_for_index()


# 텍스트 to 임베딩
def generate_embedding(text):
    embedding = openai.embeddings.create(input=[text], model='text-embedding-3-small').data[0].embedding
    return embedding

class MovieEmbeddingUpdater:
    def __init__(self, conn):
        self.conn = conn

    def add_embedding_to_movie(self):
        """모든 Movie 노드의 plot을 임베딩하고, 그 값을 embedding 속성에 추가"""
        with self.conn.driver.session(database=self.conn.database) as session:
            result = session.run(
                "MATCH (m:Movie) WHERE m.plot IS NOT NULL RETURN m.title AS title, m.plot AS plot, m.movieId AS id LIMIT 100"
            )
            cnt = 0
            for record in result:
                cnt += 1
                title = record["title"]
                plot = record["plot"]
                node_id = record["id"]

                embedding = generate_embedding(plot)  # OpenAI Embeddings 생성
                
                # 임베딩 벡터를 Neo4j에 저장
                session.run(
                    "MATCH (m:Movie) WHERE m.movieId = $id SET m.plotEmbedding = $embedding",
                    id=node_id,
                    embedding=embedding
                )
            print(cnt)
        return cnt



# 임베딩 추가 실행
updater = MovieEmbeddingUpdater(conn)
updated_count = updater.add_embedding_to_movie()
print(f"Updated {updated_count} movie nodes with embeddings.")

# 데이터에 임베딩 속성이 들어갔는지 확인하는 함수
def verify_embeddings():
    with conn.driver.session(database=conn.database) as session:
        result = session.run("MATCH (m:Movie) where m.plotEmbedding is not null RETURN m.plotEmbedding LIMIT 5")
        return any(record["m.plotEmbedding"] for record in result)

# 데이터 검증
if verify_embeddings():
    print("Embeddings are present in the database.")
else:
    raise Exception("No embeddings found in the database. Check if embeddings were added correctly.")


# 벡터 인덱스가 존재하는지 다시 확인
if not check_index_exists():
    raise Exception("Vector index 'moviePlotsEmbedding' does not exist in Neo4j. Please check the index creation.")

# 벡터 검색
embedder = OpenAIEmbeddings(model="text-embedding-3-small")
print(f"embedder {embedder} ")

retriever = VectorRetriever(
    conn.driver,
    index_name="moviePlotsEmbedding",
    embedder=embedder,
   # return_properties=["title", "plot"],
)

query_text = "A cowboy doll is jealous when a new spaceman figure becomes the top toy." # 토이스토리 줄거리
retriever_result = retriever.search(query_text=query_text, top_k=3)
print(retriever_result)