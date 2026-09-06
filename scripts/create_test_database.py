"""Run in a one-off container with MYSQL_USER=root to prepare an isolated test DB."""
from pipeline.database import connect

with connect() as connection, connection.cursor() as cursor:
    cursor.execute('CREATE DATABASE IF NOT EXISTS gitbugs_test')
    cursor.execute("GRANT ALL PRIVILEGES ON gitbugs_test.* TO 'gitbugs'@'%'")
    connection.commit()
