import psycopg2
from psycopg2 import Error


try:
    # Connect to an existing database
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="4000",
                                  database="crawler")

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
    cursor.close()
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)


def check_table_exists():
    exists = False
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'news'"
        cursor.execute(query)
        for row in cursor.fetchall():
            exists = True
    except (Exception, Error) as error:
        print("Error while reading data...", error)
    return exists

def create_table():
    try:
        cursor = connection.cursor()
        create_table_query = '''CREATE TABLE news
                  (URL varchar(255) PRIMARY KEY     NOT NULL,
                  TITLE           TEXT    NOT NULL,
                  SUB_TITLE         TEXT    NOT NULL,
                  ABSTRACT      TEXT        NOT NULL,
                  DOWNLOAD_TIME     timestamp    NOT NULL,
                  UPDATE_TIME       timestamp); '''
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")
    except (Exception, Error) as error:
        print("Error while creating Table", error)


def check_news_exists(link):
    exists = False
    try:
        cursor = connection.cursor()
        select_query = "SELECT * from news where URL='"+link+"'"
        cursor.execute(select_query)
        for row in cursor.fetchall():
            exists = True
    except (Exception, Error) as error:
        print("Error while reading data...", error)
    return exists


def insert_scraped_data(link, title, sub_title, abstract, download_time):
    try:
        cursor = connection.cursor()
        insert_query = "INSERT into news values('"+link+"', '"+title+"', '"+sub_title+"', '"+abstract+"', '"+download_time+"')"
        # Execute a command: this creates a new table
        cursor.execute(insert_query)
        connection.commit()
        print("Data inserted successfully in PostgreSQL ")
    except (Exception, Error) as error:
        print("Error while inserting data...", error)


def update_scraped_time(link, download_time):
    try:
        cursor = connection.cursor()
        update_query = "UPDATE news set UPDATE_TIME='"+download_time+"' where URL='"+link+"'"
        # Execute a command: this creates a new table
        cursor.execute(update_query)
        connection.commit()
        print("Data updated successfully in PostgreSQL ")
    except (Exception, Error) as error:
        print("Error while updating data...", error)