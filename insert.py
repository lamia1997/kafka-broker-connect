import psycopg2
import pandas as pd

word_table_drop = "DROP TABLE IF EXISTS worldcupm;"

# CREATE TABLES

world_table_create = ("""
CREATE TABLE IF NOT EXISTS public.worldcupm
(
    Year text,
    Country text,
    Winner text,
    RunnersUp text,
    Third text,
    Fourth text,
    GoalsScored text,
    QualifiedTeams text,
    MatchesPlayed text,
    Attendance text
);
""")

world_table_insert = ("""
INSERT INTO worldcupm(Year,Country,Winner,RunnersUp,Third,Fourth,GoalsScored,QualifiedTeams,MatchesPlayed,Attendance) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

# QUERY LISTS

create_table_queries = [world_table_create]
drop_table_queries = [word_table_drop]
insert_table_queries = [world_table_insert]
def create_database():

    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=kafkaproject user=postgres password=1997")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS worldcupm")
    cur.execute("CREATE DATABASE worldcupm WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=kafkaproject user=postgres password=1997")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn, forme):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in insert_table_queries:
        cur.execute(query, forme)
        conn.commit()

def dataframe():
    data2 = pd.read_csv("WorldCups.csv", delimiter= ',')
    return data2

def main():

    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    data3 = dataframe()
    for v in data3.values:
       insert_tables(cur, conn, v)

   
    


    conn.close()


if __name__ == "__main__":
    main()