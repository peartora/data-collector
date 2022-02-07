import pymysql

def get_connection():
    return pymysql.Connect(
        user = 'lsm', 
        password = 'lsm123', 
        host = '10.14.2.8', 
        database = 'clinching_process'
    )





'''
import pymssql

server = '127.0.0.1'
database = 'clinching_process'
username = 'lsm1dae'
password = 'Welcome$4556'

def get_connection():
    return pymssql.connect(server, username, password, database)
'''