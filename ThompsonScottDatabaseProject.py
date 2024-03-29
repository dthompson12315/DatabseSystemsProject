import sqlite3
from sqlite3 import Error
from faker import Faker
from random import randint
import time
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
                            building_number integer,
                            street_name text,
                            city text,
                            state text,
                            postalcode integer,
                            max_capacity integer
                        );
                        """
        createTable(conn, createQuery)

        #create event table
        createQuery = """CREATE TABLE IF NOT EXISTS event (
                            event_ID integer PRIMARY KEY AUTOINCREMENT,
                            time text,
                            topic text,
                            number_of_papers integer,
                            attendees_interested_in_topic integer,
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

        fillOriginalDatabse(conn)

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
        venue_row = (fake.company(), fake.building_number(), fake.street_name(), fake.city(), fake.state_abbr(include_territories=True), fake.postalcode(), randint(100, 10000))
        insertSql = "INSERT INTO venue (venue_name, building_number, street_name, city, state, postalcode, max_capacity) VALUES {};".format(venue_row)
        insertValues(conn, insertSql)

        #add a row to the event table
        event_row = (str(fake.date_time()), fake.sentence(), randint(1, 5), randint(100,1000), randint(1,100))
        insertSql = "INSERT INTO event (time, topic, number_of_papers, attendees_interested_in_topic, venue_ID) VALUES {};".format(event_row)
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
        #create venue table
        createQuery = """CREATE TABLE IF NOT EXISTS venue (
                            venue_ID integer PRIMARY KEY AUTOINCREMENT,
                            venue_name text,
                            max_capacity integer,
                            postalcode integer,
                            FOREIGN KEY(postalcode) REFERENCES address
                        );
                        """
        createTable(conn, createQuery)

        #create event table
        createQuery = """CREATE TABLE IF NOT EXISTS event (
                            event_ID integer PRIMARY KEY AUTOINCREMENT,
                            time text,
                            topic text,
                            number_of_papers integer,
                            attendees_interested_in_topic integer,
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

        #create addresses table
        createQuery = """CREATE TABLE IF NOT EXISTS addresses (
                            postalcode integer PRIMARY KEY,
                            building_number integer,
                            street_name text,
                            city text,
                            state text
                        );
                        """
        createTable(conn, createQuery)

        conn.close()

        fillDecomp1()

    else:
        print("An error occured while creating the decomp1 databse.")

def fillDecomp1():
    #copy necessary data from venue table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT venue_name, max_capacity, postalcode
                FROM venue;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO venue (venue_name, max_capacity, postalcode) VALUES {};".format(row)
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from event table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM event;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO event (time, topic, number_of_papers, attendees_interested_in_topic, venue_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from author table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM author;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO author (name, paper_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################
    
    #copy data from papers table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM papers;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO papers (title, topic, author_ID, event_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from reviewers table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM reviewers;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO reviewers (name, paper_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from chairs table to the decomp1 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM chairs;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO chairs (weight_limit, room_number, event_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from venue table to the decomp1 addresses table
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT postalcode, building_number, street_name, city, state
                FROM venue;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp1.sqlite")
    for row in rows:
        insertSql = "INSERT INTO addresses (postalcode, building_number, street_name, city, state) VALUES {};".format(row)
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################


def createDecomp2():
    conn = connectToDB("decomp2.sqlite")
    if conn:
        #create venue table
        createQuery = """CREATE TABLE IF NOT EXISTS venue (
                            venue_ID integer PRIMARY KEY AUTOINCREMENT,
                            venue_name text,
                            max_capacity integer,
                            postalcode integer,
                            FOREIGN KEY(postalcode) REFERENCES address
                        );
                        """
        createTable(conn, createQuery)

        #create event table
        createQuery = """CREATE TABLE IF NOT EXISTS event (
                            event_ID integer PRIMARY KEY AUTOINCREMENT,
                            time text,
                            number_of_papers integer,
                            topic_ID integer,
                            venue_ID integer,
                            FOREIGN KEY(venue_ID) REFERENCES venue,
                            FOREIGN KEY(topic_ID) REFERENCES topic
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

        #create addresses table
        createQuery = """CREATE TABLE IF NOT EXISTS addresses (
                            postalcode integer PRIMARY KEY,
                            building_number integer,
                            street_name text,
                            city text,
                            state text
                        );
                        """
        createTable(conn, createQuery)

        #create topics table
        createQuery = """CREATE TABLE IF NOT EXISTS topics (
                            topic_ID integer PRIMARY KEY AUTOINCREMENT,
                            topic text,
                            attendees_interested_in_topic integer
                        );
                        """
        createTable(conn, createQuery)

        conn.close()
        fillDecomp2()

    else:
        print("An error occured while creating the decomp2 databse.")

def fillDecomp2():
    #copy necessary data from venue table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT venue_name, max_capacity, postalcode
                FROM venue;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO venue (venue_name, max_capacity, postalcode) VALUES {};".format(row)
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from event table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM event;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    i = 1
    for row in rows:
        #put the topic data into the topic table
        insertSql = "INSERT INTO topics (topic, attendees_interested_in_topic) VALUES {};".format((row[2], row[4]))
        insertValues(conn, insertSql)
        #put the rest of the event table into to event table
        insertSql = "INSERT INTO event (time, number_of_papers, topic_ID, venue_ID) VALUES {};".format((row[1], row[3], i, row[5]))
        insertValues(conn, insertSql)
        i += 1

    conn.close()
    ###################################################################

    #copy data from author table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM author;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO author (name, paper_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################
    
    #copy data from papers table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM papers;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO papers (title, topic, author_ID, event_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from reviewers table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM reviewers;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO reviewers (name, paper_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from chairs table to the decomp2 database
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT *
                FROM chairs;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO chairs (weight_limit, room_number, event_ID) VALUES {};".format(row[1:])
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

    #copy data from venue table to the decomp2 addresses table
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    query = """SELECT postalcode, building_number, street_name, city, state
                FROM venue;
            """
    cursorObj.execute(query)
    rows = cursorObj.fetchall()
    conn.close()

    conn = connectToDB("decomp2.sqlite")
    for row in rows:
        insertSql = "INSERT INTO addresses (postalcode, building_number, street_name, city, state) VALUES {};".format(row)
        insertValues(conn, insertSql)
    conn.close()
    ###################################################################

def testQueries():
    print("Running Test Queries On Base Database:")
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    Query1 = "select author.name, papers.title, reviewers.reviewer_ID,venue.venue_name from author,reviewers,event,venue,papers where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query1).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 1 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    Query2 = "select author.name, papers.topic, event.attendees_interested_in_topic from author, papers, event where event.topic = papers.topic;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query2).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 2 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("base.sqlite")
    cursorObj = conn.cursor()
    Query3 = "select author.name, papers.title, venue.building_number, venue.postalcode, chairs.chair_ID, reviewers.reviewer_ID, venue.venue_name from author,reviewers,event,venue,papers,chairs where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query3).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 3 time: {}\n".format(total_time/1000))
    conn.close()

    print("Running Test Queries On Decomp1 Database:")
    conn = connectToDB("decomp1.sqlite")
    cursorObj = conn.cursor()
    Query1 = "select author.name, papers.title, reviewers.reviewer_ID, venue.venue_name from author,reviewers,event,venue,papers where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query1).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 1 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("decomp1.sqlite")
    cursorObj = conn.cursor()
    Query2 = "select author.name, papers.topic, event.attendees_interested_in_topic from author, papers, event where event.topic = papers.topic;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query2).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 2 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("decomp1.sqlite")
    cursorObj = conn.cursor()
    Query3 = "select author.name, papers.title, addresses.building_number, addresses.postalcode, chairs.chair_ID, venue.postalcode, reviewers.reviewer_ID, venue.venue_name from author,reviewers,event,venue,papers,chairs,addresses where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID AND venue.postalcode = addresses.postalcode order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query3).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 3 time: {}\n".format(total_time/1000))
    conn.close()

    print("Running Test Queries On Decomp2 Database:")
    conn = connectToDB("decomp2.sqlite")
    cursorObj = conn.cursor()
    Query1 = "select author.name,papers.title,reviewers.reviewer_ID,venue.venue_name from author,reviewers,event,venue,papers where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query1).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 1 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("decomp2.sqlite")
    cursorObj = conn.cursor()
    Query2 = "select author.name, papers.topic, topics.attendees_interested_in_topic, topics.topic from author, papers, topics where topics.topic = papers.topic;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query2).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 2 time: {}".format(total_time/1000))
    conn.close()
    
    conn = connectToDB("decomp2.sqlite")
    cursorObj = conn.cursor()
    Query3 = "select author.name, papers.title, addresses.building_number, addresses.postalcode, chairs.chair_ID, venue.postalcode, reviewers.reviewer_ID, venue.venue_name from author,reviewers,event,venue,papers,chairs,addresses where author.author_ID = papers.author_ID AND reviewers.paper_ID = papers.paper_ID AND papers.event_ID = event.event_ID AND venue.venue_ID = event.venue_ID AND venue.postalcode = addresses.postalcode order by author.name;"
    total_time = 0
    for i in range(1000):
        start_time = time.time() 
        cursorObj.execute(Query3).fetchall()
        end_time = time.time()
        total_time += end_time - start_time
    print("Average Query 3 time: {}\n".format(total_time/1000))
    conn.close()
    

#Utility function to add tables to database
def createTable(conn, createTableSql):
    try:
        c = conn.cursor()
        c.execute(createTableSql)
    except Error as e:
        print(createTableSql)
        print(e)

#Utility function to insert values into tables
def insertValues(conn, insertSql):
    try:
        c = conn.cursor()
        c.execute(insertSql)
        conn.commit()
    except Error as e:
        print(insertSql)
        print("Error inserting values. Error: {}".format(e))

#utility function to connect to db and return connection object
def connectToDB(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

createDatabases()
testQueries()