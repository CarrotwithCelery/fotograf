# #!/usr/bin/env python3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
import os


class MySQL:
    """ connection info for database """
    db_engine = 'mysql'
    host = 'host.docker.internal'
    database = 'codetest'
    user = 'codetest'
    password = 'swordfish'


def get_connection():
    """ connect to database using parameters from MySQL Class """
    engine = f'{MySQL.db_engine}://{MySQL.database}:{MySQL.password}@{MySQL.host}/{MySQL.user}'
    engine = create_engine(engine)
    Session = sessionmaker(bind=engine)

    return engine, Session()

def load_data(sql, conn):
    """ csv file, table name, and database connection as arguments to load into respective tables """

    result = conn.execute(sql)
    rows = result.fetchall()

    data = {row[1]: row[0] for row in rows}

    return data


if __name__ == '__main__':

    query = text("SELECT COUNT(given_name),country "
                 "FROM people a "
                 "INNER JOIN places b "
                 "ON a.place_of_birth=b.city "
                 "GROUP BY country "
                 "ORDER BY COUNT(given_name)")

    filename = '/data/summary_output.json'
    
    try:
        engine, session = get_connection()
        conn = engine.connect()
        output = load_data(query, conn)

        with open(filename, 'w') as f:
            json.dump(output, f)
            print(output)
            print(f'Results saved successfully to {filename}')

        session.close()

    except Exception as e:
        print(e)
