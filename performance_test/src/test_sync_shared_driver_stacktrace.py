import time
import signal
import sys
import traceback
from neo4j import GraphDatabase
import os
from concurrent.futures import ThreadPoolExecutor

# 상대 경로를 사용하여 config 폴더 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.dbconfig import DB_CONFIG  # 별도 설정 파일에서 접속 정보 불러오기

# 로그 파일 설정
LOG_FILE = "database_test_sync.log"

def log_message(message):
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{message}\n")
    print(message)  # 터미널에도 출력

# 연결 풀 설정
config = {
    "max_connection_pool_size": 50,        # default : 100
    "max_connection_lifetime": 1800,  # 30분  default : 60분
    "liveness_check_timeout": 30,  # 30초     
    "connection_acquisition_timeout": 30  # 30초
}

# 전역 드라이버 변수
shared_driver = None

# 드라이버 초기화 함수
def init_driver(uri, auth):
    global shared_driver
    if shared_driver is None:
        shared_driver = GraphDatabase.driver(uri, auth=auth, **config)
    return shared_driver

# 드라이버 종료 함수
def close_driver():
    global shared_driver
    if shared_driver is not None:
        shared_driver.close()
        shared_driver = None

# 데이터베이스 테스트 함수
def test_database_connection(database):
    start_time = time.time()
    log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting database test for {database}...")
    try:
        with shared_driver.session(database=database) as session:
            result = session.run("MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)")
            # records = [record for record in result]
            log_message(f"Test Query Result from {database} database: {records}")
    except Exception as e:
        log_message(f"Database Connection Test Failed for {database} database: {e}")
        log_message(traceback.format_exc())  # 스택 트레이스를 로그에 기록
    finally:
        end_time = time.time()
        log_message(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database test completed in {end_time - start_time:.2f} seconds.")

# 여러 개의 테스트 실행 함수
def run_multiple_tests(concurrent_tasks):
    # 드라이버 초기화
    init_driver(DB_CONFIG["uri"], DB_CONFIG["auth"])
    
    with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
        futures = [executor.submit(test_database_connection, DB_CONFIG["database"]) for _ in range(concurrent_tasks)]
        try:
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            log_message("Keyboard Interrupt detected. Shutting down gracefully...")
            executor.shutdown(wait=False)
            sys.exit(0)
        finally:
            # 드라이버 종료
            close_driver()

# SIGINT 핸들러 설정
def signal_handler(sig, frame):
    log_message("Keyboard Interrupt detected. Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        run_multiple_tests(concurrent_tasks=12)  # 동시 실행할 개수 지정
    except KeyboardInterrupt:
        log_message("Process interrupted. Exiting...")
        sys.exit(0)