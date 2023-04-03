from neo4j import GraphDatabase, basic_auth

# uri = "neo4j://localhost:7687"
# USER_NAME = "neo4j" kyeongmin_kang
# PASSWORD = "neo4jqwert" 
URI = "neo4j+s://dfe8e34d.databases.neo4j.io:7687" 
USER_NAME = "sanghyun_kim" 
PASSWORD = "sanghyun_kim" 

# Connection object 생성 
 
driver = GraphDatabase.driver(URI, auth = basic_auth(USER_NAME, PASSWORD))


def create_person(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)

def create_friend_of(tx, name, friend):
    tx.run("MATCH (a:Person) WHERE a.name = $name "
           "CREATE (a)-[:KNOWS]->(:Person {name: $friend})",
           name=name, friend=friend)

with driver.session() as session:
    session.write_transaction(create_person, "Alice")
    session.write_transaction(create_friend_of, "Alice", "Bob")
    session.write_transaction(create_friend_of, "Alice", "Carl")

driver.close()

