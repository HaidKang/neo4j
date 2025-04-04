import asyncio
import time
from neo4j import AsyncGraphDatabase
from config import AURA_DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기

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

# 드라이버 생성
def get_driver(uri, auth):
    return AsyncGraphDatabase.driver(uri, auth=auth, **config)

# 데이터베이스 테스트 함수 (Aura에서는 database 선택 필요 없음)
async def test_database_connection(uri, auth):
    driver = get_driver(uri, auth)
    start_time = time.time()
    log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test...")
    try:
        async with driver.session() as session:
            # 기존 쿼리 실행
            result = await session.run("MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)")
            records = [record async for record in result]
            log_message(f"Test Query Result: {records}")
            
            # kkm_con_count 노드 업데이트
            await session.run(
                "MERGE (c:kkm_con_count) "
                "ON CREATE SET c.count = 1, c.last_insert = datetime() "
                "ON MATCH SET c.count = c.count + 1, c.last_insert = datetime()"
            )
    except Exception as e:
        log_message(f"Database Connection Test Failed: {e}")
    finally:
        await driver.close()
        end_time = time.time()
        log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds.")

# 동시 실행을 위한 여러 개의 테스트 함수 실행
async def run_multiple_tests(concurrent_tasks):
    tasks = [test_database_connection(AURA_DB_CONFIG["uri"], AURA_DB_CONFIG["auth"]) for _ in range(concurrent_tasks)]
    await asyncio.gather(*tasks)

async def main():
    await run_multiple_tests(concurrent_tasks=1)  # 동시 실행할 개수 지정

if __name__ == "__main__":
    asyncio.run(main())
