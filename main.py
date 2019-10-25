import cx_Oracle as ora
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
import uuid
import threading as th

PARTS = 10
ARRSIZE = 2000


def get_min_max(pool, table, column):
    conn = pool.acquire()
    cursor = conn.cursor()
    minmax = """select min(t.{}), max(t.{}) from (select * from {}) t"""
    minmax = minmax.format(column, column, table)
    cursor.execute(minmax)
    x = cursor.fetchone()
    cursor.close()
    pool.release(conn)
    return x


def get_data(pool, query):
    conn = pool.acquire()
    cursor = conn.cursor()
    cursor.arraysize = ARRSIZE
    df = pd.read_sql_query(con=conn, sql=query)
    cursor.close()
    pool.release(conn)
    return df


def getCotas(min, max, parts):
    cant = max - min
    part = int(cant / parts)
    ct = []
    while min < max:
        lmin = min
        lmax = min + part
        min = lmax + 1
        ct.append([lmin, lmax])
    ct[-1][1] = max
    return ct


def getPart(pool, cota):
    id = str(uuid.uuid4()) + '.parquet'
    conn = pool.acquire()
    cursor = conn.cursor()
    cursor.arraysize = ARRSIZE
    qry = "select * from tickets where violationprecinct between {} and {}".format(*cota)
    df = get_data(pool, qry)
    table = pa.Table.from_pandas(df=df)
    fs = pa.hdfs.connect(host='hdp', port=9000, user='hdfs')
    with fs.open('/user/hdfs/tickets.python/'+id, 'wb') as fw:
        parquet.write_table(table=table, where=fw)
    cursor.close()
    pool.release(conn)


if __name__ == "__main__":
    dsn = ora.makedsn(host='10.0.0.2', port='1521', service_name='FISCO.matinet')
    pool = ora.SessionPool(user="SPARK_METASTORE", password="SPARK_METASTORE", dsn=dsn, min=PARTS, max=100, increment=1,
                           encoding="UTF-8", threaded=True, getmode=ora.SPOOL_ATTRVAL_WAIT)
    min1, max1 = get_min_max(pool, 'tickets', 'violationprecinct')
    print("min: " + str(min1))
    print("max: " + str(max1))
    cotas = getCotas(min1, max1, PARTS)
    threads = list()
    for cota in cotas:
        threads.append(th.Thread(target=getPart, args=[pool, cota]))
    for x in threads:
        x.start()
    for x in threads:
        x.join()
    pool.close()
