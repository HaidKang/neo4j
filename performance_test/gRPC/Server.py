import grpc
from concurrent import futures
import neo4j_service_pb2
import neo4j_service_pb2_grpc
import asyncio
import logging
from test_async_shared_driver import execute_query
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jServiceServicer(neo4j_service_pb2_grpc.Neo4jServiceServicer):
    async def ExecuteQuery(self, request, context):
        try:
            query = request.query
            start_time = time.time()
            # logger.info(f"Received query: {query}")
            result = await execute_query(query)
            # logger.info(f"Query result: {result}")
            end_time = time.time()
            total_time = end_time - start_time
            
            # Convert timestamps to datetime for better readability
            start_time_str = time.strftime('%H:%M:%S', time.localtime(start_time))
            end_time_str = time.strftime('%H:%M:%S', time.localtime(end_time))

            logger.info(f"Test server completed in {total_time:.2f} sec , "
                        f"start_time {start_time_str}, "
                        f"end_time {end_time_str}")
            
            return neo4j_service_pb2.QueryResponse(result=str(result))
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return neo4j_service_pb2.QueryResponse()

def serve():
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=20))
    neo4j_service_pb2_grpc.add_Neo4jServiceServicer_to_server(Neo4jServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    return server

async def main():
    server = serve()
    await server.start()
    logger.info("gRPC server is running on port 50051")
    await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(main())