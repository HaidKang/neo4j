import time
from neo4j import GraphDatabase
from config.dbconfig import DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기
from fastapi import FastAPI
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel
from typing import List

app = FastAPI()

# 연결 풀 설정
config = {
    "max_connection_pool_size": 50,        # default : 100
    "max_connection_lifetime": 1800,  # 30분  default : 60분
    "liveness_check_timeout": 30,  # 30초     
    "connection_acquisition_timeout": 30  # 30초
}

# 드라이버 생성
def get_driver(uri, auth):
    return GraphDatabase.driver(uri, auth=auth, **config)

# 데이터베이스 테스트 함수
def test_database_connection(uri, auth, database):
    driver = get_driver(uri, auth)
    start_time = time.time()
    response = {"database": database, "status": "success", "records": [], "duration": 0}
    try:
        with driver.session(database=database) as session:
            result = session.run("MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)")
            response["records"] = [record.data() for record in result]
    except Exception as e:
        response["status"] = "failed"
        response["error"] = str(e)
    finally:
        driver.close()
        end_time = time.time()
        response["duration"] = round(end_time - start_time, 2)
    return response

# 여러 개의 테스트 실행 함수
def run_multiple_tests(concurrent_tasks):
    with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
        futures = [executor.submit(test_database_connection, DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"]) for _ in range(concurrent_tasks)]
        return [future.result() for future in futures]

@app.get("/test-sync-db")
def test_db():
    concurrent_tasks = 20  # 동시 실행할 개수 지정
    results = run_multiple_tests(concurrent_tasks)
    return results
