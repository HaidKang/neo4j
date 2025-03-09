# pip install neo4j-genai
# pip install neo4j
# pip install neo4j-driver
# pip install openai
# pip install neo4j-graphrag

import os
import time
import pandas as pd
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

# 영화 추천 검색 Test Cypher 쿼리
cypher_query = '''
MATCH (m:Movie )<-[:RATED]-(u:User)-[:RATED]->(rec:Movie)
WHERE m.plotEmbedding is NOT NULL
RETURN rec.title AS recommendation LIMIT 20
'''

# 쿼리 실행
results = conn.query(cypher_query, {})

# DataFrame으로 변환 결과 출력
df = pd.DataFrame(results, columns=["recommendation"])
print(df)

# 벡터 검색
embedder = OpenAIEmbeddings(model="text-embedding-3-small")
print(f"embedder {embedder} ")

retriever = VectorRetriever(
    conn.driver,
    index_name='mPlotsEmbedding',
    embedder=embedder,
    return_properties=["title", "plot"],
)

query_text = "A cowboy doll is jealous when a new spaceman figure becomes the top toy." # 토이스토리 줄거리
retriever_result = retriever.search(query_text=query_text, top_k=3)
print(retriever_result)