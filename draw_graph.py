import numpy as np
import matplotlib.pyplot as plt

# data to plot
reports_summary = ['ma_summary.txt', 'my_summary.txt', 'pg_summary.txt']
for report_file in reports_summary:
    with open('./benchmark/' + report_file, 'r') as rf:
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
    if report_file == 'pg_summary.txt':
        postpresql = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print(postpresql)
    if report_file == 'ma_summary.txt':
        maria = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print(maria)
    if report_file == 'my_summary.txt':
        mysql = (avg_write_time, q1, q2, q3, q4, q5, q6)
        print(mysql)

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

plt.bar(index + bar_width*2, mysql, bar_width,
        alpha=opacity,
        color='r',
        label='Mysql-8.0')

plt.xlabel('Time')
plt.ylabel('Query')
plt.title('Compatible Mysql, PostpreSQL, MariaDB')


# 'q1: SELECT * FROM testing LIMIT 1000',
# "q2:SELECT * FROM testing WHERE int_col > 5000 LIMIT 1000",
# 'q3:SELECT * FROM testing WHERE int_col + int_col2 > 12345 LIMIT 1000',
# "q4:SELECT COUNT(*) FROM testing WHERE int_col + int_col2 > 12345",
# "q5:SELECT * FROM testing WHERE int_col > 5000 ORDER BY word_col ASC LIMIT 1000",
#"q6:SELECT * FROM testing WHERE word_col LIKE '%lim%' ORDER BY word_col DESC LIMIT 1000"


plt.xticks(
    index + bar_width, ('avg_write_time/10000rows',
                        'q1',
                        "q2",
                        'q3',
                        "q4",
                        "q5",
                        "q6"
                        ))
plt.legend()

plt.tight_layout()
plt.show()
