import asyncio
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from neo4j import AsyncGraphDatabase, GraphDatabase
import sys
import os
import logging
from fastapi import FastAPI, Query

# Add the parent directory of 'config' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))
from dbconfig import DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기

app = FastAPI()

# 로그 파일 설정
LOG_FILE = "database_test_async.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Thread-%(thread)d] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 연결 풀 설정
config = {
    "max_connection_pool_size": 30,
    "max_connection_lifetime": 1800,  # 30분
    "liveness_check_timeout": 30,  # 30초
    "connection_acquisition_timeout": 30  # 30초
}

# 드라이버 생성
def get_driver(uri, auth):
    return AsyncGraphDatabase.driver(uri, auth=auth, **config)

def get_sync_driver(uri, auth):
    return GraphDatabase.driver(uri, auth=auth, **config)

# XML 파일에서 쿼리 읽기
def get_query_from_xml(query_id):
    try:
        tree = ET.parse('../queries/queries.xml')
        root = tree.getroot()
        for query in root.findall('query'):
            if query.get('id') == query_id:
                return query.text.strip()
        return None
    except ET.ParseError as e:
        logger.error(f"XML Parsing Error: {e}")
        return None
    except FileNotFoundError:
        logger.error("Error: queries.xml file not found.")
        return None

# 데이터베이스 테스트 함수
async def test_database_connection(uri, auth, database, item_id, idx):
    print(f"item_id : {item_id}, idx : {idx}")
    driver = get_driver(uri, auth)
    start_time = time.time()
    logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test for {database}..., item_id : {item_id} , index : {idx}")
    try:
        async with driver.session(database=database) as session:
            query = get_query_from_xml('testQuery')
            if query:
                records = []
                result = await session.run(query)
                records = [record async for record in result]
                # logger.info(f"Test Query Result from {database} database: {records}")
                return {"status": "success", "records": records}
            else:
                logger.info(f"Query not found in XML for {database} database.")
                return {"status": "error", "message": "Query not found"}
    except Exception as e:
        logger.error(f"Database Connection Test Failed for {database} database: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        await driver.close()
        end_time = time.time()
        logger.error(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds. item_id : {item_id} , index : {idx}")

def sync_test_database_connection(uri, auth, database, item_id, idx):
    print(f"item_id : {item_id}, idx : {idx}")
    driver = get_sync_driver(uri, auth)
    start_time = time.time()
    logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test for {database}..., item_id : {item_id} , index : {idx}")
    try:
        with driver.session(database=database) as session:
            query = get_query_from_xml('testQuery')
            if query:
                records = []
                session.run(query)
                records.append({item_id: item_id, idx: idx})
                return {"status": "success", "records": records}
            else:
                logger.info(f"Query not found in XML for {database} database.")
                return {"status": "error", "message": "Query not found"}
    except Exception as e:
        logger.error(f"Database Connection Test Failed for {database} database: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        end_time = time.time()
        logger.error(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds. item_id : {item_id} , index : {idx}")

@app.get("/")
async def root():
    return {"message": "Welcome to the FastAPI application. Visit /docs for API documentation."}

@app.get("/test_db")
async def run_database_test():
    result = await test_database_connection(DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"])
    return result

@app.get("/test_db_multiple/{item_id}")
async def run_multiple_tests(item_id: int = 1):
    process_start_time = time.time()
    logger.info(f"process_start : [{time.strftime('%Y-%m-%d %H:%M:%S')}] ,  item_id : {item_id} ")
    # print(f"start :: item_id : {item_id}")
    tasks = [test_database_connection(DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"], item_id, i) for i in range(20)]
    results = await asyncio.gather(*tasks)
    process_end_time = time.time()
    logger.info(f"process_end :  [{time.strftime('%Y-%m-%d %H:%M:%S')}] , item_id : {item_id}, Process test completed in {process_end_time - process_start_time:.2f}  ")
    # print(f"end :: item_id : {item_id}")
    return {"status": "success", "results": results}

@app.get("/async_def/{item_id}")
async def async_def_test(item_id: int = 1):
    process_start_time = time.time()
    logger.info(f"process_start : [{time.strftime('%Y-%m-%d %H:%M:%S')}] ,  item_id : {item_id} ")
    # print(f"start :: item_id : {item_id}")
    tasks = [test_database_connection(DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"], item_id, i) for i in range(20)]
    results = await asyncio.gather(*tasks)
    process_end_time = time.time()
    logger.info(f"process_end :  [{time.strftime('%Y-%m-%d %H:%M:%S')}] , item_id : {item_id}, Process test completed in {process_end_time - process_start_time:.2f}  ")
    # print(f"end :: item_id : {item_id}")
    return {"status": "success", "results": results}

async def gather_def_test(item_id, idx):
    loop = asyncio.get_event_loop()
    tasks = []
    with ThreadPoolExecutor() as executor:
        for i in range(idx):
            task = loop.run_in_executor(
                executor,
                sync_test_database_connection,
                DB_CONFIG["uri"],
                DB_CONFIG["auth"],
                DB_CONFIG["database"],
                item_id,
                i
            )
            tasks.append(task)
    return await asyncio.gather(*tasks)

@app.get("/def/{item_id}")
def def_test(item_id: int = 1):
    process_start_time = time.time()
    logger.info(f"process_start : [{time.strftime('%Y-%m-%d %H:%M:%S')}] ,  item_id : {item_id} ")
    results = asyncio.run(gather_def_test(item_id, 20))
    process_end_time = time.time()
    logger.info(f"process_end :  [{time.strftime('%Y-%m-%d %H:%M:%S')}] , item_id : {item_id}, Process test completed in {process_end_time - process_start_time:.2f}  ")
    return {"status": "success", "results": results}

async def gather_test_database_connection(item_id, idx): 
    tasks = []
    for i in range(idx):
        task = asyncio.create_task(test_database_connection(DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"], item_id, i))
        tasks.append(task)
    return await asyncio.gather(*tasks)

@app.get("/test_db_multiple_2/{item_id}")
def run_multiple_tests_2(item_id: int = 1):
    process_start_time = time.time()
    logger.info(f"process_start : [{time.strftime('%Y-%m-%d %H:%M:%S')}] ,  item_id : {item_id} ")
    # print(f"start :: item_id : {item_id}")
    results = asyncio.run(gather_test_database_connection(item_id, 20))
    process_end_time = time.time()
    logger.info(f"process_end :  [{time.strftime('%Y-%m-%d %H:%M:%S')}] , item_id : {item_id}, Process test completed in {process_end_time - process_start_time:.2f}  ")
    # print(f"end :: item_id : {item_id}")
    return {"status": "success", "results": results}

def run_multiple_tests(item_id):
    async def wrapper(item_id, idx):
        return await test_database_connection(DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"], item_id, idx)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = [loop.create_task(wrapper(item_id, i)) for i in range(20)]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
    return results

@app.get("/test_db_multiple_3/{item_id}")
def run_multiple_tests_3(item_id: int = 1):
    process_start_time = time.time()
    logger.info(f"process_start : [{time.strftime('%Y-%m-%d %H:%M:%S')}] ,  item_id : {item_id} ")
    # print(f"start :: item_id : {item_id}")
    results = run_multiple_tests(item_id)
    process_end_time = time.time()
    logger.info(f"process_end :  [{time.strftime('%Y-%m-%d %H:%M:%S')}] , item_id : {item_id}, Process test completed in {process_end_time - process_start_time:.2f}  ")
    # print(f"end :: item_id : {item_id}")
    return {"status": "success", "results": results}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
