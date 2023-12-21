import psycopg2
import pandas as pd
import time

start_time = time.time()


def read_csv():
    data = pd.read_csv("data.csv")
    data.drop(['motivation', 'prizeShare', 'bornCountry', 'bornCity', 'died', 'diedCountry', 'diedCity'], axis=1,
              inplace=True)
    data.dropna(inplace=True)
    data.reset_index(inplace=True)
    data.drop(['index'], axis=1, inplace=True)
    data.reset_index(inplace=True)
    data.rename(columns={'index': 'id'}, inplace=True)

    return data


def db_connect():
    db_params = {
        "host": "localhost",
        "database": "NobelPrizeWinners",
        "user": "Shkarupylo",
        "password": "Shkarupylo",
        "port": "5432",
    }
    conn = psycopg2.connect(**db_params)
    return conn


def create_prize_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS prize CASCADE;")
        cur.execute("""CREATE TABLE Prize
                    (
                      year INT NOT NULL,
                      category VARCHAR NOT NULL,
                      id INT NOT NULL,
                      PRIMARY KEY (id)
                    );""")


def create_organisation_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS organization CASCADE;")
        cur.execute("""CREATE TABLE Organization
                    (
                      organizationName VARCHAR NOT NULL,
                      organizationCountry VARCHAR NOT NULL,
                      organizationCity VARCHAR NOT NULL,
                      PRIMARY KEY (organizationName)
                    );""")


def create_laureate_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS laureate CASCADE;")
        cur.execute("""CREATE TABLE Laureate
                    (
                      laureateID INT NOT NULL,
                      fullName VARCHAR NOT NULL,
                      gender VARCHAR NOT NULL,
                      born DATE NOT NULL,
                      organizationName VARCHAR NOT NULL,
                      PRIMARY KEY (laureateID),
                      FOREIGN KEY (organizationName) REFERENCES Organization(organizationName)
                    );""")


def create_prizelaureates_table(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS prizelaureates CASCADE;")
        cur.execute("""CREATE TABLE PrizeLaureates
                    (
                      id INT NOT NULL,
                      laureateID INT NOT NULL,
                      PRIMARY KEY (id, laureateID),
                      FOREIGN KEY (id) REFERENCES Prize(id),
                      FOREIGN KEY (laureateID) REFERENCES Laureate(laureateID)
                    );""")


def insert_organization(cur, organizationName, organizationCountry, organizationCity):
    cur.execute(
        "SELECT COUNT(*) FROM organization WHERE organizationName = '{}'".format(organizationName.replace("'", '')))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(
            "INSERT INTO organization (organizationName, organizationCountry, organizationCity) VALUES (%s, %s, %s)",
            (organizationName, organizationCountry, organizationCity))


def insert_laureate(cur, laureateID, fullName, gender, born, organizationName):
    if isinstance(born, str):
        born = born.replace("'", '')
        born = f"{born}"
        if (len(born.split('-')) == 3 or (len(born.split('/')) == 3)) and len(born) <= 12:
            born = born.replace('00', '01')
    cur.execute("SELECT COUNT(*) FROM laureate WHERE laureateID = '{}'".format(laureateID))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(
            "INSERT INTO laureate (laureateID, fullName, gender, born, organizationName) VALUES (%s, %s, %s, %s, %s)",
            (laureateID, fullName, gender, born, organizationName))


def insert_prize(cur, id, year, category):
    cur.execute("SELECT COUNT(*) FROM prize WHERE id = %s AND year = %s AND category = %s",
                (id, year, category))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("INSERT INTO prize (id, year, category) VALUES (%s, %s, %s)",
                    (id, year, category))


def insert_prizelaureates(cur, id, laureateID):
    cur.execute("SELECT COUNT(*) FROM prizelaureates WHERE id = %s AND laureateID = %s",
                (id, laureateID))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("INSERT INTO prizelaureates (id, laureateID) VALUES (%s, %s)",
                    (id, laureateID))


if __name__ == "__main__":
    data = read_csv()
    conn = db_connect()
    create_prize_table(conn)
    create_laureate_table(conn)
    create_organisation_table(conn)
    create_prizelaureates_table(conn)
    start_time = time.time()
    num = 0
    for index, row in data.iterrows():
        if index == 10001:
            break
        with conn:
            cur = conn.cursor()
        if int(index) % 1000 == 0:
            elapsed_time = time.time() - start_time
            print(f"Imported {index}, Elapsed Time: {round(elapsed_time, 2)} seconds")
            start_time = time.time()

        insert_organization(cur, row['organizationName'], row['organizationCountry'], row['organizationCity'])
        insert_laureate(cur, row['laureateID'], row['fullName'], row['gender'], row['born'], row['organizationName'])
        insert_prize(cur, row['id'], row['year'], row['category'])
        insert_prizelaureates(cur, row['id'], row['laureateID'])
        num = index

    elapsed_time = time.time() - start_time
    print(f"Imported {num}, Elapsed Time: {round(elapsed_time, 2)} seconds")