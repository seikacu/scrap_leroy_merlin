import csv

import psycopg

from secure import PSql, log


def connect_db():
    connection = psycopg.connect(
        host=PSql.host,
        user=PSql.user,
        password=PSql.password,
        dbname=PSql.db_name
    )
    return connection


def check_exist_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('leroymerlin',))
        return cursor.fetchone()[0]


def create_table_ads(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE leroymerlin (
                    id serial NOT NULL,
                    url varchar(200) NOT NULL UNIQUE,
                    launch_point varchar(100) NOT NULL,
                    photos varchar(1000),
                    name varchar(250),
                    article varchar(20),
                    price varchar(15),
                    category varchar(100),
                    sub_category varchar(100),
                    section varchar(100),
                    description varchar(1000),
                    price_per varchar(15),
                    brand varchar(50),                    
                    path_page varchar(200),                    
                    CONSTRAINT "leroymerlin_pk" PRIMARY KEY ("id","url")
                    ) WITH (
                    OIDS=FALSE
                );"""
            )

            print("[INFO] Table created successfully")

    except Exception as _ex:
        # log.write_log("db_sql_create_table_ads ", _ex)
        print("db_sql_create_table_ads_ Error while working with PostgreSQL", _ex)
        pass


def insert_to_table(connection, url, category, sub_category_1, sub_category_2, sub_category_3, sub_category_4,
                    sub_category_5, location, launch_point):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO ads (url, category, sub_category_1, sub_category_2, sub_category_3, sub_category_4,
                sub_category_5, location, launch_point) VALUES 
                    ('{url}', '{category}', '{sub_category_1}', '{sub_category_2}', '{sub_category_3}',
                    '{sub_category_4}', '{sub_category_5}', '{location}', '{launch_point}');"""
            )

    except Exception as _ex:
        # log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass

def insert_url_table(connection, url, launch_point):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO leroymerlin (url, launch_point) VALUES 
                    ('{url}', '{launch_point}');"""
            )
    except Exception as _ex:
        # log.write_log("db_sql_insert_to_table ", _ex)
        print("db_sql_insert_to_table_  Error while working with PostgreSQL", _ex)
        pass


def add_path_page(connection, id_db, path_page):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE leroymerlin SET path_page = '{path_page}' WHERE id = {id_db};""")

            print(f"[INFO] Path_page {path_page} was successfully add")

    except Exception as _ex:
        # log.write_log("db_sql_add_phone1 ", _ex)
        print("db_sql__Path_page Error while working with PostgreSQL", _ex)
        pass


def add_phone2(connection, id_db, phone):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE ads SET phone_2 = '{phone}' WHERE id = {id_db};""")

            print(f"[INFO] Phone_2 {phone} was successfully add")

    except Exception as _ex:
        log.write_log("db_sql_add_phone2 ", _ex)
        print("db_sql_phone2_ Error while working with PostgreSQL", _ex)
        pass


def get_data_to_csv_file(name_csv):
    connection = None
    split_name_csv = name_csv.split("_")
    try:
        connection = connect_db()
        with connection.cursor() as cursor:
            sql = f"""SELECT * FROM ads WHERE launch_point = '{split_name_csv[0]}'"""
            cursor.execute(sql)
            rows = cursor.fetchall()
            with open(f"result/{name_csv}.csv", "a", newline='', encoding="utf-8") as file:
                writer = csv.writer(file, delimiter='\t')
                writer.writerows(rows)
    except Exception as _ex:
        log.write_log("db_sql_get_data_to_csv_file ", _ex)
        print("db_sql_get_data_to_csv_file_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] Данные выгружены в CSV файл")


def check_url_in_bd(connection, url):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT url FROM leroymerlin WHERE url = '{url}';""")
        return cursor.fetchone() is not None


def delete_data_from_table(category_name):
    connection = None
    try:
        connection = connect_db()
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"""DELETE FROM ads WHERE launch_point = '{category_name}';""")
            print("[INFO] Data was deleted")
    except Exception as _ex:
        log.write_log("db_sql_delete_data_from_table ", _ex)
        print("db_sql_delete_data_from_table_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def delete_table():
    connection = None
    try:
        connection = connect_db()
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"""DROP TABLE IF EXISTS ads;""")
            print("[INFO] TABLE was deleted")
    except Exception as _ex:
        log.write_log("db_sql_delete_table ", _ex)
        print("db_sql_delete_table_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def get_data_from_table(connection, category_name):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, url FROM ads WHERE launch_point = '{category_name}'
        AND data IS NULL;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()


def get_links_from_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, url FROM leroymerlin WHERE path_page IS NULL;""")
        # cursor.execute(f"""SELECT * FROM leroymerlin;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()

def get_id_from_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""SELECT id, path_page FROM leroymerlin WHERE path_page IS NOT NULL;""")
        if cursor.fetchone is not None:
            return cursor.fetchall()
