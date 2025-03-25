# config.py
DB_CONFIG = {
    "uri": "neo4j+s://c75ff9e1.databases.neo4j.io",
    "auth": ("neo4j", "aU-p2chkqK-m2kAva4YnuVhPlW5zRijxlakCJe8dVK4"),
    "database": "neo4j"
}

CLUSTER_DB_CONFIG = {
    "uri": "neo4j://10.103.8.151:7687",
    "auth": ("neo4j", "neo4j.123456"),
    "database": "idle2e20250118"
}


DEV_DB_CONFIG = {
    "uri": "bolt://10.103.8.137:7687",
    "auth": ("neo4j", "neo4j.123456"),
    "database": "idle2e20241012"
}

AURA_DB_CONFIG = {
    "uri": "neo4j+s://c75ff9e1.databases.neo4j.io",
    "auth": ("neo4j", "aU-p2chkqK-m2kAva4YnuVhPlW5zRijxlakCJe8dVK4")
}
