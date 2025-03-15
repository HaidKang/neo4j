from neo4j import GraphDatabase
import time

class Neo4jDataMigration:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def migrate_data(self):
        query = """
        MATCH (o:OceanRoute)
        WITH o LIMIT 20000
        CREATE (k:kkm01 {
          acmCostAmount: o.acmCostAmount,
          costAmount: o.costAmount,
          costErrorFlag: o.costErrorFlag,
          costEvaluationScoreDry: o.costEvaluationScoreDry,
          costEvaluationScoreRf: o.costEvaluationScoreRf,
          costPriority: o.costPriority,
          destinationPodCode: o.destinationPodCode,
          destinationPodRegionCode: o.destinationPodRegionCode,
          directionCodes: o.directionCodes,
          dominantFlag: o.dominantFlag,
          equipmentCostAmount: o.equipmentCostAmount,
          id_TMP: o.id_TMP,
          oceanRouteUid: o.oceanRouteUid,
          originPolCode: o.originPolCode,
          originPolRegionCode: o.originPolRegionCode,
          pastLiftVolAmtDry: o.pastLiftVolAmtDry,
          pastLiftVolAmtRf: o.pastLiftVolAmtRf,
          portCodes: o.portCodes,
          rankDry: o.rankDry,
          rankRf: o.rankRf,
          rccCodes: o.rccCodes,
          rccCombinationName: o.rccCombinationName,
          relationshipIds: o.relationshipIds,
          revenueLaneCodes: o.revenueLaneCodes,
          serviceLaneCodes: o.serviceLaneCodes,
          serviceScopeCode: o.serviceScopeCode,
          serviceScopeGroupCode: o.serviceScopeGroupCode,
          sumTesCostAmount: o.sumTesCostAmount,
          sumTrsCostAmount: o.sumTrsCostAmount,
          top10Dry: o.top10Dry,
          top10Rf: o.top10Rf,
          totalCnnHours: o.totalCnnHours,
          totalDwellHours: o.totalDwellHours,
          totalDwellTransitTimeHours: o.totalDwellTransitTimeHours,
          totalTransitTimeHours: o.totalTransitTimeHours,
          transshipmentCount: o.transshipmentCount,
          trunkLaneCode: o.trunkLaneCode,
          ttEvaluationScoreDry: o.ttEvaluationScoreDry,
          ttEvaluationScoreRf: o.ttEvaluationScoreRf,
          validPortFlag: o.validPortFlag,
          vesselCostAmount: o.vesselCostAmount,
          wayportTeuQtys: o.wayportTeuQtys
        })
        RETURN count(k) AS kkm01NodesCreated;
        """
        
        start_time = time.time()
        with self.driver.session(database="preorm") as session:
            result = session.run(query)
            for record in result:
                end_time = time.time()
                print(f"Execution Time: {(end_time - start_time) * 1000:.2f} ms")
                print(f"kkm01 Nodes Created: {record['kkm01NodesCreated']}")

if __name__ == "__main__":
    # Replace with your Neo4j credentials
    uri = "bolt://10.103.2.139:7687"
    # uri = "bolt://10.103.2.152:7687"
    # uri = "bolt://10.103.2.154:7687"    
    
    user = "neo4j"
    password = "neo4j.123456"
    
    migration = Neo4jDataMigration(uri, user, password)
    migration.migrate_data()
    migration.close()
