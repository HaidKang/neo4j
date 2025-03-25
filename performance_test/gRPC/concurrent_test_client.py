import grpc
import neo4j_service_pb2
import neo4j_service_pb2_grpc
import asyncio
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

async def make_request(stub, request_id):
    try:
        start_time = time.time()
        # query = "MATCH (n) RETURN n LIMIT 1"
        # query = "MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)"
        query = "MATCH p= (n:FirstPOL)<-[:MOVES_TO]-(i:Inland) RETURN count(p)"
        request = neo4j_service_pb2.QueryRequest(query=query)
        response = await stub.ExecuteQuery(request)
        end_time = time.time()
        logger.info(f"Request {request_id} completed in {end_time - start_time:.2f} seconds")
        return response
    except Exception as e:
        logger.error(f"Request {request_id} failed: {str(e)}")
        return None

async def run_concurrent_test(num_requests):
    channel = grpc.aio.insecure_channel('localhost:50051')
    stub = neo4j_service_pb2_grpc.Neo4jServiceStub(channel)
    
    logger.info(f"Starting concurrent test with {num_requests} requests")
    start_time = time.time()
    
    # Create multiple requests
    tasks = [make_request(stub, i) for i in range(num_requests)]
    
    # Wait for all requests to complete
    responses = await asyncio.gather(*tasks)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Convert end_time to a readable format
    end_time_str = time.strftime('%H:%M:%S', time.localtime(end_time))
    
    # Log results
    success_count = sum(1 for r in responses if r is not None)
    logger.info(f"Test completed in {total_time:.2f} seconds")
    logger.info(f"Successful requests: {success_count}/{num_requests}")
    logger.info(f"Test completed at {end_time_str}")
    
    await channel.close()

if __name__ == "__main__":
    asyncio.run(run_concurrent_test(10))