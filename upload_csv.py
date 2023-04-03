from neo4j import GraphDatabase, basic_auth 
 
URI = "neo4j+s://dfe8e34d.databases.neo4j.io:7687" 
USER_NAME = "sanghyun_kim" 
PASSWORD = "sanghyun_kim" 
 
''' 
URI = "bolt://localhost:7687" 
USER_NAME = "neo4j" 
PASSWORD = "Sang!!8525" 
''' 
 
# Connection object 생성 
 
dirver = GraphDatabase.driver( 
    URI, 
    auth = basic_auth(USER_NAME, PASSWORD) 
) 
 
''' 
with dirver.session() as session: 
    with session.begin_transaction() as tx: 
        inputCypher = """ 
                      merge (n:WORD {seq: $seq}) 
                      on match 
                      set n.word = $word 
                      return count(n) 
                      """ 
        results = tx.run(inputCypher, parameters={"seq":"4", "word":"A/R"}) 
        print(f"Result: >>> {results.single()[0]}") 
 
        inputCypher = """ 
                      merge (n:WORD {seq: $seq}) 
                      on match 
                      set n.word = $word 
                      return count(n) 
                      """ 
        results = tx.run(inputCypher, parameters={"seq":"1", "word":"A/A"}) 
        tx.Commit() 
        print(f"Result: >>> {results.single()[0]}") 
''' 
 
with dirver.session(database="neo4j") as session: 
    results = session.run("match (n:WORD) return n, n.word as word limit 10") 
 
    for row in results: 
        print(f"row >>> {row}") 
        print(f"Word >>> {row['word']}") 
 
dirver.close()