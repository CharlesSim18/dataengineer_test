#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import module
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
  
# assign dataset names
list_of_names = ['customer','lineitem','nation','orders','part','partsupp','region','supplier']
  
# create empty list
dataframes_list = []
  
# append datasets into the list
for i in range(len(list_of_names)):
    temp_df = pd.read_csv("mysql\\"+list_of_names[i]+".csv", header=None, index_col=False, delimiter = ',')
    dataframes_list.append(temp_df)

customer = dataframes_list[0]
lineitem = dataframes_list[1]
nation = dataframes_list[2]
orders = dataframes_list[3]
part = dataframes_list[4]
partsupp = dataframes_list[5]
region = dataframes_list[6]
supplier = dataframes_list[7]

#Create Schema in MySQL Workbench
try:
    scdb = msql.connect(
        host="localhost",
        user="root",
        password="12345"    #Password set in your database
    )
    if scdb.is_connected():
        cursor = scdb.cursor()
        cursor.execute("CREATE DATABASE cooee1_db")
        print("Created Database")
except Error as e:
    print("Connection error to MySQL", e)
    
try:
    scdb = msql.connect(
        host="localhost",
        user="root",
        password="12345",    #Password set in your database
        database="cooee1_db"
    )
    if scdb.is_connected():
        cursor = scdb.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Connected to databse: ", record)
        #cursor.execute('DROP TABLE IF EXISTS customer;')
        cursor.execute('DROP TABLE IF EXISTS lineitem;')
        print('Creating tables..')
        cursor.execute("CREATE TABLE customer(c_custkey INT NOT NULL, c_name TEXT NOT NULL,            c_address TEXT NOT NULL, c_nationkey INT NOT NULL, c_phone TEXT NOT NULL, c_acctbal INT NOT NULL,            c_mktsegment TEXT NOT NULL, c_comment TEXT NOT NULL, PRIMARY KEY (c_custkey))")
        cursor.execute("CREATE TABLE orders(o_orderkey INT PRIMARY KEY NOT NULL, o_custkey INT NOT NULL, o_orderstatus TEXT NOT NULL,            o_totalprice INT NOT NULL, o_orderdate TEXT NOT NULL, o_orderpriority TEXT NOT NULL, o_clerk TEXT NOT NULL,            o_shippriority INT NOT NULL, o_comment TEXT NOT NULL, FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey))")
        cursor.execute("CREATE TABLE region(r_regionkey INT NOT NULL, r_name TEXT NOT NULL, n_comment TEXT,            PRIMARY KEY (r_regionkey))")
        cursor.execute("CREATE TABLE nation(n_nationkey INT NOT NULL, n_name TEXT NOT NULL, n_regionkey INT NOT NULL,            n_comment TEXT, PRIMARY KEY (n_nationkey), FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey))")
        cursor.execute("CREATE TABLE supplier(s_suppkey INT PRIMARY KEY NOT NULL, s_name TEXT NOT NULL, s_address TEXT NOT NULL,            s_nationkey INT NOT NULL, s_phone TEXT NOT NULL, s_acctbal INT NOT NULL, s_comment TEXT NOT NULL,            FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey))")
        cursor.execute("CREATE TABLE part(p_partkey INT PRIMARY KEY NOT NULL, p_name TEXT NOT NULL, p_mfgr TEXT NOT NULL,            p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size INT NOT NULL, p_container TEXT NOT NULL,            p_retailprice INT NOT NULL, p_comment TEXT NOT NULL)")
        cursor.execute("CREATE TABLE partsupp(ps_partkey INT NOT NULL, ps_suppkey INT NOT NULL, ps_availqty INT NOT NULL,            ps_supplycost INT NOT NULL, ps_comment TEXT NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey),            FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey), FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey))")
        cursor.execute("CREATE TABLE lineitem(l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL,            l_linenumber INT NOT NULL, l_quantity INT NOT NULL, l_extendedprice INT NOT NULL, l_discount INT NOT NULL,            l_tax INT NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL,            l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL,            l_shipmode TEXT NOT NULL, l_comment TEXT NOT NULL, PRIMARY KEY (l_orderkey, l_linenumber),            FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),            FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey))")
        print("Table is created....")
        #looping
        for i,row in customer.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.customer VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Customer Table Finished")
        for i,row in orders.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Orders Table Finished")
        for i,row in region.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.region VALUES (%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Region Table Finished")
        for i,row in nation.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.nation VALUES (%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Nation Table Finished")
        for i,row in supplier.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.supplier VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Supplier Table Finished")
        for i,row in part.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.part VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("Part Table Finished")
        for i,row in partsupp.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.partsupp VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("PartSupp Table Finished")
        for i,row in lineitem.iterrows():
            #here %s means string values
            sql = "INSERT INTO cooee1_db.lineitem VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # Committing changes
            scdb.commit()
        print("LineItem Table Finished")
except Error as e:
            print("Connection error to MySQL", e)
           


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




