import numpy as np
import matplotlib.pyplot as plt
from enum import Enum

# data to plot
class ReportFile(Enum):
    MARIA_DB = 'mariadb_summary.txt'
    MYSQL = 'mysql_summary.txt'
    POSTPRESQL = 'postgres_summary.txt'
    MONGO_DB = 'mongodb_summary.txt'

# reports_summary = ['mariadb_sumary.txt', 'mysql_sumary.txt', 'pg_summary.txt']
for report_file in ReportFile:
    with open('./benchmark/' + report_file.value, 'r') as rf:
        lines = rf.readlines()
        for line in lines:
            line = line.split(':')
            if line[0] == "avg_write_time(ms/10000rows)":
                avg_write_time = float(line[1])
            if line[0] == "avg_query_q1_time(ms)":
                q1 = float(line[1])
            if line[0] == "avg_query_q2_time(ms)":
                q2 = float(line[1])
            if line[0] == "avg_query_q2_time(ms)":
                q3 = float(line[1])
            if line[0] == "avg_query_q4_time(ms)":
                q4 = float(line[1])
            if line[0] == "avg_query_q5_time(ms)":
                q5 = float(line[1])
            if line[0] == "avg_query_q6_time(ms)":
                q6 = float(line[1])
    if report_file == ReportFile.POSTPRESQL:
        postpresql = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print("postpresql {}".format(postpresql))
    if report_file == ReportFile.MARIA_DB:
        maria = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print('maria_db {}'.format(maria))
    if report_file == ReportFile.MYSQL:
        mysql = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print('mysql {}'.format(mysql))
    if report_file == ReportFile.MONGO_DB:
        mongo_db = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print('mongo_db {}'.format(mongo_db))

n_groups = 7
# create plot
fig, ax = plt.subplots()
index = np.arange(n_groups)
bar_width = 0.2
opacity = 0.8

plt.bar(index, postpresql, bar_width,
        alpha=opacity,
        color='b',
        label='PostgreSql-11.2')

plt.bar(index + bar_width, maria, bar_width,
        alpha=opacity,
        color='g',
        label='MariaDB-10.3')

plt.bar(index + bar_width * 2, mysql, bar_width,
        alpha=opacity,
        color='r',
        label='Mysql-8.0')

plt.bar(index + bar_width * 3, mongo_db, bar_width,
        alpha=opacity,
        color='y',
        label='MongoDB-4.0.9')

plt.xlabel('Query')
plt.ylabel('Time(ms)')
plt.title('Compatible Mysql, PostpreSQL, MariaDB, MongoDB')

plt.xticks(
    index + bar_width, ('avg_write',
                        'q1',
                        "q2",
                        'q3',
                        "q4",
                        "q5",
                        "q6"
                        ))
plt.legend()
plt.tight_layout()
plt.savefig('benchmark_mongo_maria_my_post_gre.png')
plt.show()
