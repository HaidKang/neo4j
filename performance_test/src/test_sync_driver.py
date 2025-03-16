import time
import signal
import sys
from neo4j import GraphDatabase
from config import DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기
from concurrent.futures import ThreadPoolExecutor

# 로그 파일 설정
LOG_FILE = "database_test_sync.log"

def log_message(message):
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{message}\n")

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
    log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test for {database}...")
    try:
        with driver.session(database=database) as session:
            # result = session.run("MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)")
            result = session.run("CYPHER runtime=parallel MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)")
            records = [record for record in result]
            log_message(f"Test Query Result from {database} database: {records}")

    except Exception as e:
        log_message(f"Database Connection Test Failed for {database} database: {e}")
    finally:
        driver.close()
        end_time = time.time()
        log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds.")

# 여러 개의 테스트 실행 함수
def run_multiple_tests(concurrent_tasks):
    with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
        futures = [executor.submit(test_database_connection, DB_CONFIG["uri"], DB_CONFIG["auth"], DB_CONFIG["database"]) for _ in range(concurrent_tasks)]
        try:
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            log_message("Keyboard Interrupt detected. Shutting down gracefully...")
            executor.shutdown(wait=False)
            sys.exit(0)

# SIGINT 핸들러 설정
def signal_handler(sig, frame):
    log_message("Keyboard Interrupt detected. Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        run_multiple_tests(concurrent_tasks=50)  # 동시 실행할 개수 지정
    except KeyboardInterrupt:
        log_message("Process interrupted. Exiting...")
        sys.exit(0)
