# -*- coding: UTF-8 -*-
import logging
import traceback

import pymysql
from pymysql.err import error_map
from redis import Redis

pymysql.install_as_MySQLdb()

logger = logging.getLogger(__name__)

conn = pymysql.connect(
    host='rm-uf60h543gj6e017lhpo.mysql.rds.aliyuncs.com',
    port=3306,
    user='fst_admin',
    password='Fst@123456',
    database='tyfst',
    charset='utf8'
)
cursor = conn.cursor()

local_redis = Redis(host='127.0.0.1', port=6379)
SYNCED_KEY = 'synced_{table_name}'


def show_sql_statement(func):
    def deco(*args, **kwargs):
        results = func(*args, **kwargs)

        sql_statement = cursor._executed
        logger.warning('Current Execute SQL Statement: %s', sql_statement)

        return results

    return deco


@show_sql_statement
def get_all_tables(dbname=None):
    if dbname is None:
        dbname = conn.db

    sql = "SELECT TABLE_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s"
    cursor.execute(sql, (dbname, ))
    db_results = cursor.fetchall()

    return [
        dict(table_name=table_name, row_num=row_num)
        for table_name, row_num in db_results
    ]


def migrate_tables(dbname=None):
    if dbname is None:
        dbname = conn.db

    table_list = get_all_tables(dbname=dbname)

    # 库下某表对应的字段
    sql = """
        SELECT
            TABLE_SCHEMA AS db_name,                        -- 库名
            TABLE_NAME AS table_name,                       -- 表名
            COLUMN_NAME AS column_name,                     -- 列名
            ORDINAL_POSITION AS ordinal_position,           -- 列的排列顺序
            COLUMN_DEFAULT AS column_default,               -- 默认值
            IS_NULLABLE AS is_nullable,                     -- 是否为空
            DATA_TYPE AS date_type,                         -- 数据类型
            CHARACTER_MAXIMUM_LENGTH AS char_max_length,    -- 字符最大长度
            NUMERIC_PRECISION AS numeric_precision,         -- 数值精度(最大位数)
            NUMERIC_SCALE AS numeric_scale,                 -- 小数精度
            COLUMN_TYPE AS column_type,                     -- 列类型
            COLUMN_KEY 'KEY',
            EXTRA AS extra,                                 -- 额外说明
            COLUMN_COMMENT AS column_comment                -- 注释
        FROM
            information_schema.`COLUMNS`
        WHERE
            TABLE_SCHEMA = %s AND table_name=%s
        ORDER BY
            TABLE_NAME,
            ORDINAL_POSITION;
    """

    for tb_info in table_list:
        table_name = tb_info['table_name']
        cursor.execute(sql, (dbname, table_name))
        db_results = cursor.fetchall()

        column_list = [row[2] for row in db_results]
        query_sql = f"SELECT {', '.join(column_list)} FROM {table_name}"
        logger.warning('Table name: %s, Query SQL: %s', table_name, query_sql)

        cursor.execute(query_sql)
        db_query_results = cursor.fetchall()
        data_list = [dict(zip(column_list, row)) for row in db_query_results]

        synced_key = SYNCED_KEY.format(table_name=table_name)
        if not local_redis.get(synced_key):
            insert_local_database(table_name, data_list=data_list)
            local_redis.set(synced_key, 'synced')


def insert_local_database(table_name, data_list):
    local_conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='Qaz123',
        database='tyfst',
        charset='utf8'
    )
    local_cursor = local_conn.cursor()

    for row_value in data_list:
        columns = list(row_value.keys())
        values = [row_value[k] for k in columns]
        value_fmts = ", ".join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES({value_fmts})"

        try:
            local_cursor.execute(sql, tuple(values))
            logger.warning('Local DB Table: %s, Insert SQL: %s', table_name, cursor._executed)
        except Exception as e:
            logger.warning('Local DB Table: %s, Insert Error >>>>>>>>')
            logger.warning(traceback.format_exc())
        else:
            local_conn.commit()


if __name__ == '__main__':
    migrate_tables()
