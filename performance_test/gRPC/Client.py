import grpc
import neo4j_service_pb2
import neo4j_service_pb2_grpc
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run():
    # Connect to the gRPC 
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = neo4j_service_pb2_grpc.Neo4jServiceStub(channel)
        start_time = time.time()
        # Create a request
        # query = "MATCH (n) RETURN n LIMIT 1"
        # query = "MATCH (n) RETURN DISTINCT labels(n), size(labels(n)), count(*)"
        query = "MATCH p= (n:FirstPOL)<-[:MOVES_TO]-(i:Inland) RETURN count(p)"
        request = neo4j_service_pb2.QueryRequest(query=query)
        
        # Make the call
        try:
            start_time = time.time()
            response = stub.ExecuteQuery(request)
            end_time = time.time()
            logger.info(f"completed in {end_time - start_time:.2f} seconds")
            logger.info(f"Query result: {response.result}")
        except grpc.RpcError as e:
            logger.error(f"gRPC call failed: {e}")

if __name__ == "__main__":
    run()