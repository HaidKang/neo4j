# https://neo4j.com/docs/python-manual/current/query-simple/

from neo4j import GraphDatabase
import os

# Set up a neo4j driver instance
uri = "neo4j://35.226.169.213:7687"
username = "neo4j"
password = "neo4jtest"
driver = GraphDatabase.driver(uri, auth=(username, password))

# 접속 Test
driver.verify_connectivity()


def init_driver(uri, username, password):
    # Create an instance of the driver
    current_app.driver = GraphDatabase.driver(uri, auth=(username, password))

    # Verify Connectivity
    current_app.driver.verify_connectivity()

    return current_app.driver


def get_people(tx):
    result = tx.run("MATCH (p:Person) RETURN p.name AS name")
    records = list(result)  # a list of Record objects
    summary = result.consume()
    return records, summary



with driver.session(database="neo4j") as session:
    records, summary = session.execute_read(get_people)

    # Summary information
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))

    # Loop through results and do something with them
    for person in records:
        print(person.data())  # obtain record as dict