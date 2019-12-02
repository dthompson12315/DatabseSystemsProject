import sqlite3
from sqlite3 import Error
from faker import Faker
from random import randint
fake = Faker()

def createDatabases():
    try:
        createOriginalDatabase()
        createDecomp1()
        createDecomp2()
        print("Databases created successfully.")
    except:
        print("An error occured while creating the databases.")

def createOriginalDatabase():
    conn = connectToDB("base.sqlite")
    if conn:
        #create venue table
        createQuery = """CREATE TABLE IF NOT EXISTS venue (
                            venue_ID integer PRIMARY KEY AUTOINCREMENT,
                            venue_name text,
                            address text,
                            max_capacity integer
                        );
                        """
        createTable(conn, createQuery)

        #create event table
        createQuery = """CREATE TABLE IF NOT EXISTS event (
                            event_ID integer PRIMARY KEY AUTOINCREMENT,
                            time text,
                            title text,
                            number_of_papers integer,
                            venue_ID integer,
                            FOREIGN KEY(venue_ID) REFERENCES venue
                        );
                        """
        createTable(conn, createQuery)

        #create author table
        createQuery = """CREATE TABLE IF NOT EXISTS author (
                            author_ID integer PRIMARY KEY AUTOINCREMENT,
                            name text,
                            paper_ID integer,
                            FOREIGN KEY(paper_ID) REFERENCES paper
                        );
                        """
        createTable(conn, createQuery)

        #create papers table
        createQuery = """CREATE TABLE IF NOT EXISTS papers (
                            paper_ID integer PRIMARY KEY AUTOINCREMENT,
                            title text,
                            topic text,
                            author_ID integer,
                            event_ID integer,
                            FOREIGN KEY(author_ID) REFERENCES author,
                            FOREIGN KEY(event_ID) REFERENCES event
                        );
                        """
        createTable(conn, createQuery)

        #create reviewers table
        createQuery = """CREATE TABLE IF NOT EXISTS reviewers (
                            reviewer_ID integer PRIMARY KEY AUTOINCREMENT,
                            name text,
                            paper_ID integer,
                            FOREIGN KEY(paper_ID) REFERENCES paper
                        );
                        """
        createTable(conn, createQuery)

        #create chairs table
        createQuery = """CREATE TABLE IF NOT EXISTS chairs (
                            chair_ID integer PRIMARY KEY AUTOINCREMENT,
                            weight_limit integer,
                            room_number integer,
                            event_ID integer,
                            FOREIGN KEY(event_ID) REFERENCES event
                        );
                        """
        createTable(conn, createQuery)

        #close the databse connection after creating all of the tables
        conn.close()
    else:
        print("An error occured while creating the base databse.")

#fill the database with fake data
def fillOriginalDatabse(conn):
    if not conn:
        return
    for i in range(100):
        #add a row to the venue table
        venue_row = (fake.company(), fake.address(), randint(100, 10000))
        insertSql = "INSERT INTO venue (venue_name, address, max_capacity) VALUES {};".format(venue_row)
        insertValues(conn, insertSql)

        #add a row to the event table
        event_row = (str(fake.date_time()), fake.sentence(), randint(1, 5), randint(1,100))
        insertSql = "INSERT INTO event (time, title, number_of_papers, venue_ID) VALUES {};".format(event_row)
        insertValues(conn, insertSql)

        #add a row to the author table
        author_row = (fake.name(), i)
        insertSql = "INSERT INTO author (name, paper_ID) VALUES {};".format(author_row)
        insertValues(conn, insertSql)

        #add a row to the papers table
        paper_row = (fake.sentence(4), fake.sentence(), i, randint(1, 30))
        insertSql = "INSERT INTO papers (title, topic, author_ID, event_ID) VALUES {};".format(paper_row)
        insertValues(conn, insertSql)

        #add a row to the reviewers table
        reviewer_row = (fake.name(), randint(1, 30))
        insertSql = "INSERT INTO reviewers (name, paper_ID) VALUES {};".format(reviewer_row)
        insertValues(conn, insertSql)

        #add a row to the chairs table
        chair_row = (randint(100, 350), randint(1, 30), randint(1, 20))
        insertSql = "INSERT INTO chairs (weight_limit, room_number, event_ID) VALUES {};".format(chair_row)
        insertValues(conn, insertSql)
    conn.close()

def createDecomp1():
    conn = connectToDB("decomp1.sqlite")
    if conn:
        conn.close()
    else:
        print("An error occured while creating the decomp1 databse.")

def createDecomp2():
    conn = connectToDB("decomp2.sqlite")
    if conn:
        conn.close()
    else:
        print("An error occured while creating the decomp2 databse.")


#Utility function to add tables to database
def createTable(conn, createTableSql):
    try:
        c = conn.cursor()
        c.execute(createTableSql)
    except Error as e:
        print(createTableSql)
        print(e)

def insertValues(conn, insertSql):
    try:
        c = conn.cursor()
        c.execute(insertSql)
        conn.commit()
    except Error as e:
        print(insertSql)
        print("Error inserting values. Error: {}".format(e))

def connectToDB(db_file):
    conn = None

    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

createDatabases()
fillOriginalDatabse(connectToDB("base.sqlite"))