import time
import logging
import bson
import os
import json
import enum
import pymongo

import psycopg2
import MySQLdb
from pymongo import MongoClient, InsertOne

from bson import json_util
from enum import Enum


# logger = logging.getLogger(__name__)


class DBMS(Enum):
    MARIADB = 'mariadb'
    MONGODB = 'mongodb'
    POSTGRESQL = 'postgres'
    MYSQL = 'mysql'


class STORE_TYPE(Enum):
    NOSQL = 'nosql'
    SQL = 'sql'


CONNECTION_INIT = {
    DBMS.POSTGRESQL.value: {
        'host': '127.0.0.1',
        'user': 'postgres',
        'passwd': 'password',
        'port': 5433
    },
    DBMS.MYSQL.value: {
        'host': '127.0.0.1',
        'user': 'root',
        'passwd': 'password',
        'port': 3302
    },
    DBMS.MARIADB.value: {
        'host': '127.0.0.1',
        'user': 'root',
        'passwd': 'password',
        'port': 3307
    },
    DBMS.MONGODB.value: {
        'host': '127.0.0.1',
        'port': 27018
    }
}


def current_mills_time(): return time.time() * 1000


class Benchmark:
    def __init__(self, db_type, arr_dict, write_dir='benchmark', db='benchmark'):
        self.dbms = db_type  # database management system
        self.time_unit = 'ms'
        self.read_query = {
            "q1": "SELECT * FROM testing LIMIT 1000",
            "q2": "SELECT * FROM testing WHERE int_col > 5000 LIMIT 1000",
            "q3": "SELECT * FROM testing WHERE int_col + int_col2 > 12345 LIMIT 1000",
            "q4": "SELECT COUNT(*) FROM testing WHERE int_col + int_col2 > 12345",
            "q5": "SELECT * FROM testing WHERE int_col > 5000 ORDER BY word_col ASC LIMIT 1000",
            "q6": "SELECT * FROM testing WHERE word_col LIKE '%lim%' ORDER BY word_col DESC LIMIT 1000"
        }
        self.query = {}
        self.db_mode = STORE_TYPE.SQL  # include sql and no-sql
        if self.dbms == DBMS.POSTGRESQL:
            self.db_initfile = 'postgres.sql'
            self.query['truncate'] = 'TRUNCATE TABLE testing RESTART IDENTITY'
            self.query['config'] = 'show all;'
        elif self.dbms == DBMS.MONGODB:
            self.db_mode = STORE_TYPE.NOSQL
        else:  # mysql or mariadb`
            self.db_initfile = 'mysql.sql'
            self.query['truncate'] = 'TRUNCATE TABLE testing'
            self.query['config'] = 'show variables;'

        self.write_dir = write_dir
        # get config of connection
        self.host = arr_dict.get('host', '127.0.0.1')
        self.user = arr_dict.get('user', None)
        self.passwd = arr_dict.get('passwd', None)
        self.port = arr_dict.get('port', None)
        self.db = db
        # self.init()

    # def init(self):
    #     if self.dbms == DBMS.POSTGRESQL:
    #         self.pg_init()
    #     elif self.dbms == DBMS.MYSQL:
    #         self.my_init()
    #     elif self.dbms == DBMS.MARIADB:
    #         self.my_init()
    #     elif self.dbms == DBMS.MONGODB:
    #         self.mo_init()

    # def my_init(self):
    #     mc = self._connect()
    #     r = mc.cursor()
    #     with open(self.db_initfile, 'r') as myfile:
    #         data = myfile.read().replace('\n', ' ')
    #     r.execute(data)
    #     r.close()
    #     print("mysql benchmark table + DB init successfully")

    # def mo_init(self):
    #     client = self._connect()
    #     db = client['benchmark']  # create database
    #     db.testing  # create collation
    #     print("mongodb benchmark DB:{},collection:{} init successfully".format(
    #         'benchmark', 'testing'))

    # def ma_init(self):
    #     mc = self._connect()
    #     r = mc.cursor()
    #     with open(self.db_initfile, 'r') as myfile:
    #         data = myfile.read().replace('\n', ' ')
    #     r.execute(data)
    #     r.close()
    #     print("MariaBB benchmark table + DB init successfully")

    # def pg_init(self):
    #     pc = self._connect()
    #     # PostgreSQL can not drop databases within a transaction,
    #     pc.set_isolation_level(0)
    #     r = pc.cursor()
    #     with open(self.db_initfile, 'r') as myfile:
    #         query = myfile.read().replace('\n', ' ')
    #     query = query.split(';')  # postgres create database is strict
    #     r.execute(query[0] + ';')
    #     r.close()
    #     pc = self._connect_db()
    #     r = pc.cursor()
    #     r.execute(query[1] + ';')
    #     r.close()
    #     pc.commit()
    #     print("postgres benchmark table + DB successfully")

    def truncate(self):
        if self.db_mode == STORE_TYPE.SQL:
            conn = self._connect_db()
            r = conn.cursor()
            r.execute(self.query['truncate'])
            r.close()
            conn.commit()
            conn.close()
        else:
            db = self._connect_db()

    def write(self):
        self.truncate()
        if self.db_mode == STORE_TYPE.SQL:
            self._sql_write()
        else:
            self._no_sql_write()

    def _no_sql_write(self):
        data = []
        with open('nosql/data.json', 'r') as data_insert_file:
            data = json.load(data_insert_file)
            iter = 100
            _write_file_report = os.path.join(
                self.write_dir,  self.dbms.value + '_write.txt')
            data = [InsertOne(d)  for d in data]
            with open(_write_file_report, 'wb') as wf:
                for _ in range(0, iter):
                    conn = self._connect_db()
                    coll = conn.testing
                    bulk = coll.bulk_write(data, bypass_document_validation=False)
                    # bulk.bulkWrite(data)
                    # bulk.execute()
                    t1 = current_mills_time()
                    # coll.bulkWrite(data)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')

    def _sql_write(self):
        with open('sql/insert.sql', 'r') as myfile:
            query = myfile.read().replace('\n', '')
            iter = 100
            _write_file_report = os.path.join(
                self.write_dir,  self.dbms.value + '_write.txt')
            with open(_write_file_report, 'wb') as wf:
                for _ in range(0, iter):
                    conn = self._connect_db()
                    r = conn.cursor()
                    t1 = current_mills_time()
                    r.execute(query)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                    r.close()
                    conn.commit()
                    conn.close()

    def read(self):
        if self.db_mode == STORE_TYPE.SQL:
            for key in self.read_query:
                self._query(self.read_query[key], key)
        else:
            # pass
            for key in self.read_query:
                self._query_mongodb(key, key)

    def _query(self, query, filename):
        iter = 100
        out_put_file = os.path.join(
            self.write_dir,  self.dbms.value + '_' + filename + '.txt')
        with open(out_put_file, 'wb') as wf:
            for _ in range(0, iter):
                conn = self._connect_db()
                r = conn.cursor()
                t1 = current_mills_time()
                r.execute(query)
                t2 = current_mills_time()
                time_used = t2 - t1
                wf.write(str(time_used) + '\n')
                r.close()
                conn.close()

    def _query_mongodb(self, query_key, filename):
        iter = 100
        out_put_file = os.path.join(
            self.write_dir,  self.dbms.value + '_' + filename + '.txt')
        with open(out_put_file, 'wb') as wf:
            for _ in range(0, iter):
                conn = self._connect_db()
                coll = conn.testing
                if query_key == 'q1':
                    t1 = current_mills_time()
                    coll.find().limit(1000)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                if query_key == 'q2':
                    t1 = current_mills_time()
                    #  "SELECT * FROM testing WHERE int_col > 5000 LIMIT 1000",
                    coll.find({'int_col': {"$gt": 5000}}).limit(1000)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                if query_key == 'q3':
                    t1 = current_mills_time()
                    # "q3": "SELECT * FROM testing WHERE int_col + int_col2 > 12345 LIMIT 1000",
                    # coll.find({ $where: "/^.*test.*$/.test(this.int_col + this.int_col2)" } )).limit(1000)
                    coll.find({ "$where": "this.int_col + this.int_col2  > 12345" } ).limit(1000)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                if query_key == 'q4':
                    # "q4": "SELECT COUNT(*) FROM testing WHERE int_col + int_col2 > 12345",
                    t1 = current_mills_time()
                    coll.find({ "$where": "this.int_col + this.int_col2  > 12345" }).count()
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                if query_key == 'q5':
                    # "q5": "SELECT * FROM testing WHERE int_col > 5000 ORDER BY word_col ASC LIMIT 1000",
                    t1 = current_mills_time()
                    coll.find({'int_col': {"$gt": bson.int64.Int64(5000)}}).sort(
                        [("word_col", pymongo.ASCENDING)]).limit(1000)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')
                if query_key == 'q6':
                    # "q6": "SELECT * FROM testing WHERE word_col LIKE '%lim%' ORDER BY word_col DESC LIMIT 1000"
                    t1 = current_mills_time()
                    # coll.find({"word_col": {"$regex": u"lim"}}).sort(
                    #     [("word_col", pymongo.ASCENDING)]).limit(1000)
                    coll.find({"word_col":  "/.*lim.*/"}).sort(
                        [("word_col", pymongo.DESCENDING)]).limit(1000)
                    t2 = current_mills_time()
                    time_used = t2 - t1
                    wf.write(str(time_used) + '\n')

    def config(self):
        config_file_locate = os.path.join(
            self.write_dir, self.dbms.value + '_config.txt')
        if self.dbms != DBMS.MONGODB:
            with open(config_file_locate, 'wb') as wf:
                conn = self._connect_db()
                r = conn.cursor()
                r.execute(self.query['config'])
                for row in r:
                    wf.write(str(row[0]) + ': ' + str(row[1]) + '\n')
                r.close()
                conn.close()
        else:
            pass
            # pass for write config of mongo-db
            # TODO: need to imp different way for mongo-db

    def summary(self):
        summary_file = os.path.join(
            self.write_dir, self.dbms.value + '_summary.txt')
        report_write_file = os.path.join(
            self.write_dir, self.dbms.value + '_write.txt')
        with open(summary_file, 'wb') as wf:
            with open(report_write_file, 'r') as f:
                lines = [float(l) for l in f.readlines() if l.strip() != '']
                avg_time = sum(lines) / len(lines)
                wf.write('avg_write_time(ms/10000rows):{}\n'.format(str(avg_time)))
                print('avg_write_time(ms/10000rows):{}'.format(str(avg_time)))
            for key in self.read_query:
                report_query_file = os.path.join(
                    self.write_dir, self.dbms.value + '_' + key + '.txt')
                with open(report_query_file, 'r') as f:
                    lines = [float(l)
                             for l in f.readlines() if l.strip() != '']
                    avg_time = sum(lines) / len(lines)
                    wf.write('avg_query_{}_time(ms):{}\n'.format(
                        key, str(avg_time)))
                    print('avg_query_{}_time(ms):{}\n'.format(key, str(avg_time)))

    def _connect(self):
        if self.dbms == DBMS.POSTGRESQL:
            return psycopg2.connect(host=self.host, user=self.user, password=self.passwd, port=self.port)
        elif self.dbms == DBMS.MONGODB:
            return MongoClient('mongodb://{}:{}'.format(self.host, self.port))
        elif self.dbms == DBMS.MYSQL or self.dbms == DBMS.MARIADB:
            return MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, port=self.port)
        else:
            raise("Cant connect to DB")

    def _connect_db(self):
        if self.dbms == DBMS.POSTGRESQL:
            return psycopg2.connect(host=self.host, user=self.user, password=self.passwd, port=self.port)
        elif self.dbms == DBMS.MONGODB:
            client = MongoClient(self.host, self.port)
            return client['benchmark']
        elif self.dbms == DBMS.MYSQL or self.dbms == DBMS.MARIADB:
            return MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port)
        else:
            raise("Cant connect to DB")


def _benchmark(database):
    # logger.info("started benchmark for {}".format(database))
    print("started benchmark for {}".format(database))
    benchmark_db = database
    dict_connection = CONNECTION_INIT[database.value]
    benchmark = Benchmark(benchmark_db, dict_connection)
    # benchmark.config()
    benchmark.write()
    benchmark.read()
    benchmark.summary()


if __name__ == "__main__":
    _benchmark(DBMS.MONGODB)
    # for database in DBMS:
    #     _benchmark(database)
        # try:

        # except Exception as e:
        #     print("Has error {}".format(e.message))
        #     # logger.error(  )
