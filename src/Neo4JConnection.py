import neo4j

class Neo4jConnection:
    # The Neo4jConnection class written by CJ Sullivan and publishe on Medium.com
    # https://medium.com/data-science/create-a-graph-database-in-neo4j-using-python-4172d40f89c4

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = neo4j.GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            print('Connection Succeeded')
        except Exception as e:
            print(f"Failed Connection:\n{e}")

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db='neo4j'):
        assert self.__driver is not None, "Driver is not initialized"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print(f"Query failure:\n{e}")
        finally:
            if session is not None:
                session.close()
        return response