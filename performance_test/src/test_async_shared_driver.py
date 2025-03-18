import asyncio
import time
import xml.etree.ElementTree as ET
from neo4j import AsyncGraphDatabase
import sys
import os

# 상대 경로를 사용하여 config 폴더 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.dbconfig import DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기

# 로그 파일 설정
LOG_FILE = "database_test_async.log"

def log_message(message):
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{message}\n")
    print(message)  # 필요하면 터미널에도 출력

# 연결 풀 설정
config = {
    "max_connection_pool_size": 50,
    "max_connection_lifetime": 1800,  # 30분
    "liveness_check_timeout": 30,  # 30초
    "connection_acquisition_timeout": 30  # 30초
}

# 전역 드라이버 변수
shared_driver = None

# 드라이버 초기화 함수
def init_driver(uri, auth):
    global shared_driver
    if shared_driver is None:
        shared_driver = AsyncGraphDatabase.driver(uri, auth=auth, **config)
    return shared_driver

# 드라이버 종료 함수
async def close_driver():
    global shared_driver
    if shared_driver is not None:
        await shared_driver.close()
        shared_driver = None

# XML 파일에서 쿼리 읽기
def get_query_from_xml(query_id):
    try:
        tree = ET.parse(os.path.abspath(os.path.join(os.path.dirname(__file__), '../queries/queries.xml')))
        root = tree.getroot()
        for query in root.findall('query'):
            if query.get('id') == query_id:
                return query.text.strip()
        return None
    except ET.ParseError as e:
        log_message(f"XML Parsing Error: {e}")
        return None
    except FileNotFoundError:
        log_message("Error: queries.xml file not found.")
        return None

# 데이터베이스 테스트 함수
async def test_database_connection(database):
    start_time = time.time()
    log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test for {database}...")
    try:
        async with shared_driver.session(database=database) as session:
            query = get_query_from_xml('testQuery')
            if query:
                result = await session.run(query)
                records = [record async for record in result]
                # log_message(f"Test Query Result from {database} database: {records}")
            else:
                log_message(f"Query not found in XML for {database} database.")
    except Exception as e:
        log_message(f"Database Connection Test Failed for {database} database: {e}")
    finally:
        end_time = time.time()
        log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds.")

# 동시 실행을 위한 여러 개의 테스트 함수 실행
async def run_multiple_tests(concurrent_tasks):
    # 드라이버 초기화
    init_driver(DB_CONFIG["uri"], DB_CONFIG["auth"])
    
    try:
        # 테스트 실행
        tasks = [test_database_connection(DB_CONFIG["database"]) for _ in range(concurrent_tasks)]
        await asyncio.gather(*tasks)
    finally:
        # 드라이버 종료
        await close_driver()

async def main():
    await run_multiple_tests(concurrent_tasks=12)  # 동시 실행할 개수 지정

if __name__ == "__main__":
    asyncio.run(main())