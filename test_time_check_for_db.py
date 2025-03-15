from neo4j import GraphDatabase
import time

class Neo4jDataMigration:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def measure_query_time(self):
        query = """
        MATCH (o:OceanRoute)
        WITH 
        o.acmCostAmount AS  acmCostAmount
      , o.costAmount  AS  costAmount
      , o.costErrorFlag AS  costErrorFlag
      , o.costEvaluationScoreDry  AS  costEvaluationScoreDry
      , o.costEvaluationScoreRf AS  costEvaluationScoreRf
      , o.costPriority  AS  costPriority
      , o.destinationPodCode  AS  destinationPodCode
      , o.destinationPodRegionCode  AS  destinationPodRegionCode
      , o.directionCodes  AS  directionCodes
      , o.dominantFlag  AS  dominantFlag
      , o.equipmentCostAmount AS  equipmentCostAmount
      , o.id_TMP  AS  id_TMP
      , o.oceanRouteUid AS  oceanRouteUid
      , o.originPolCode AS  originPolCode
      , o.originPolRegionCode AS  originPolRegionCode
      , o.pastLiftVolAmtDry AS  pastLiftVolAmtDry
      , o.pastLiftVolAmtRf  AS  pastLiftVolAmtRf
      , o.portCodes AS  portCodes
      , o.rankDry AS  rankDry
      , o.rankRf  AS  rankRf
      , o.rccCodes  AS  rccCodes
      , o.rccCombinationName  AS  rccCombinationName
      , o.relationshipIds AS  relationshipIds
      , o.revenueLaneCodes  AS  revenueLaneCodes
      , o.serviceLaneCodes  AS  serviceLaneCodes
      , o.serviceScopeCode  AS  serviceScopeCode
      , o.serviceScopeGroupCode AS  serviceScopeGroupCode
      , o.sumTesCostAmount  AS  sumTesCostAmount
      , o.sumTrsCostAmount  AS  sumTrsCostAmount
      , o.top10Dry  AS  top10Dry
      , o.top10Rf AS  top10Rf
      , o.totalCnnHours AS  totalCnnHours
      , o.totalDwellHours AS  totalDwellHours
      , o.totalDwellTransitTimeHours  AS  totalDwellTransitTimeHours
      , o.totalTransitTimeHours AS  totalTransitTimeHours
      , o.transshipmentCount  AS  transshipmentCount
      , o.trunkLaneCode AS  trunkLaneCode
      , o.ttEvaluationScoreDry  AS  ttEvaluationScoreDry
      , o.ttEvaluationScoreRf AS  ttEvaluationScoreRf
      , o.validPortFlag AS  validPortFlag
      , o.vesselCostAmount  AS  vesselCostAmount
      , o.wayportTeuQtys  AS  wayportTeuQtys 
        LIMIT 20000
        RETURN count(*) AS totalRecords
        ;
        """
        
        start_time = time.time()
        with self.driver.session(database="preorm") as session:
            result = session.run(query)
            for record in result:
                end_time = time.time()
                print(f"Execution Time: {(end_time - start_time) * 1000:.2f} ms")
                print(f"Total Records Retrieved: {record['totalRecords']}")

if __name__ == "__main__":
    # Replace with your Neo4j credentials
    uri = "neo4j://10.103.2.139:7687"
    # uri = "bolt://10.103.2.139:7687"
    # uri = "bolt://10.103.2.152:7687"
    # uri = "bolt://10.103.2.154:7687"
    user = "neo4j"
    password = "neo4j.123456"
    
    migration = Neo4jDataMigration(uri, user, password)
    migration.measure_query_time()
    migration.close()