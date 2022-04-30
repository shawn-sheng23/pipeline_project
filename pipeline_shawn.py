#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 16:54:55 2022

@author: shawn
"""

import mysql.connector
import pandas as pd

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user='root',
                                             password='SJsj@900215',
                                             host='localhost',
                                             port='3306',
                                             database='db1')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

connection = get_db_connection()

def load_third_party(connection, file_path_csv):
    try:
        cursor = connection.cursor()
        mySql_Create_Table_Query = """
        CREATE TABLE db1.third_party_sales (
        ticket_id INT NOT NULL,
        trans_date TEXT NOT NULL,
        event_id INT NOT NULL,
        event_name TEXT NOT NULL,
        event_date TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_city TEXT NOT NULL,
        customer_id INT NOT NULL,
        price FLOAT NOT NULL,
        num_tickets INT NOT NULL,
        PRIMARY KEY (ticket_id))
        """
        result = cursor.execute(mySql_Create_Table_Query)
        print("Ticket Table created successfully")
    except mysql.connector.Error as error:
        print("Failed to create table in MySQL: {}".format(error))
    df = pd.read_csv(file_path_csv)
    df.to_sql(con = connection, name='db1.third_party_sales', if_exists='replace', flavor='mysql')
    print("data ingested to db1.ticket_sales")

def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_statement = """
    SELECT event_name, SUM(num_tickets) AS total_tickets
    FROM db1.third_party_sales
    GROUP BY event_name
    ORDER BY total_tickets DESC
    LIMIT 3
    """
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

records = query_popular_tickets(connection)

def print_results(records):
    print('Top 3 most popular events are:')
    print("1. " + str(pd.DataFrame(records, columns = ['event_name', 'total_tickets'])['event_name'][0]))
    print("2. " + str(pd.DataFrame(records, columns = ['event_name', 'total_tickets'])['event_name'][1]))
    print("3. " + str(pd.DataFrame(records, columns = ['event_name', 'total_tickets'])['event_name'][2]))
    
print_results(records)