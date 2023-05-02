from neo4j import GraphDatabase

# Set up a neo4j driver instance
uri = "neo4j://35.226.169.213:7687"
username = "neo4j"
password = "neo4jtest"
driver = GraphDatabase.driver(uri, auth=(username, password))


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