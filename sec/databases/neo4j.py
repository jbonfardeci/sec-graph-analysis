from py2neo import Graph
import os, sys
import json
from types import SimpleNamespace

def get_connection(ip_address, pwd, port="7687", user="neo4j", trace=False):
    """
    Creates and returns Neo4J Graph instance.
    """
    try:
        db = Graph(scheme="bolt", host=ip_address, port=port, user=user, password=pwd)
        if trace:
            print("Connection success!")
        return db
    except Exception as e:
        raise e

def get_credentials():
    """
    Get local login credentials for Neo4j instance.
    """
    with open(f'{os.getcwd()}/credentials/neo4j.json', 'r') as file:
        creds = json.load(file, object_hook=lambda d: SimpleNamespace(**d))
        return creds

def test():
    creds = get_credentials().local
    db: Graph = get_connection(creds.ip, creds.password)
    if db:
        print("Connection test successful!")
    else:
        print("Connection test failed!")