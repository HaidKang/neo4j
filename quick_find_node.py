from neo4j import GraphDatabase

# Set up a neo4j driver instance
uri = "neo4j://35.226.169.213:7687"
username = "neo4j"
password = "neo4jtest"
driver = GraphDatabase.driver(uri, auth=(username, password))

# def get_friends_of(tx, name):
#     friends = []
#     result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
#                     "WHERE a.name = $name "
#                     "RETURN f.name AS friend", name=name)
#     for record in result:
#         friends.append(record["friend"])
#     return friends
query = "MATCH (a:Person)-[:KNOWS]->(f) WHERE a.name = $name RETURN f.name AS friend"
def get_friends_of(tx, name, query):
    friends = []
    result = tx.run(query, name=name)
    for record in result:
        friends.append(record["friend"])
    return friends

with driver.session() as session:
    friends = session.read_transaction(get_friends_of, "Alice", query)
    for friend in friends:
        print(friend)

driver.close()