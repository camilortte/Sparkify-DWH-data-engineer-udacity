"""
This moudel generate a ER diagram for the sparkifydb
"""
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
import configparser

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')    
    conn_string = "postgresql://{db_user}:{db_password}@{host}:{db_port}/{db_name}".format(**dict(config['CLUSTER']))

    graph = create_schema_graph(metadata=MetaData(conn_string))
    graph.write_png('sparkifydb_erd.png')

if __name__ == "__main__":
    main()