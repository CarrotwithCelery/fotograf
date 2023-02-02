# #!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


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


class MyTable1(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    given_name = Column(String(50))
    family_name = Column(String(50))
    date_of_birth = Column(String(50))
    place_of_birth = Column(String(50))


class MyTable2(Base):
    __tablename__ = 'places'
    city = Column(String(50), primary_key=True)
    county = Column(String(50), primary_key=True)
    country = Column(String(50), primary_key=True)


def load_data(data, db_table, connection):
    """ csv file, table name, and database connection as arguments to load into respective tables """
    df = pd.read_csv(data)
    df.to_sql(name=db_table, con=connection, if_exists="replace", index=False)

    print(f'File "{data}" loaded successfully to table "{db_table}"')

    session.commit()


if __name__ == '__main__':

    try:
        engine, session = get_connection()
        Base.metadata.create_all(engine)

        file_and_table = {'people.csv': 'people',
                          'places.csv': 'places'}

        for file, table in file_and_table.items():
            load_data(file, table, engine)

        session.close()

    except Exception as e:
        print(e)
