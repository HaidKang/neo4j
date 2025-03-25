from neo4j import AsyncGraphDatabase

# 전역 변수로 설정값 정의
URI = "bolt://10.103.8.137:7687"
USER = "neo4j"
PASSWORD = "neo4j.123456"
DATABASE = "idle2e20241012"

# 전역 driver 인스턴스 생성 - 이 부분이 driver를 공유하게 만듦
driver = AsyncGraphDatabase.driver(
    URI, 
    auth=(USER, PASSWORD),
    max_connection_lifetime=30,
    max_connection_pool_size=20
)

async def execute_query(query):
    # 기존에 생성된 driver 재사용
    async with driver.session(database=DATABASE) as session:
        result = await session.run(query)
        records = await result.data()
        return str(records)

async def close_driver():
    # 프로그램 종료 시 driver 정리
    await driver.close()

if __name__ == "__main__":
    import asyncio
    print("KMKang , 개별 query Test")
    query = "MATCH (n) RETURN n LIMIT 1"
    try:
        result = asyncio.run(execute_query(query))
        print(result)
    finally:
        asyncio.run(close_driver())

