import random

f = open('word_list.txt', 'r')
wordlist = []
for line in f:
    wordlist.append(line.strip())


def generate_sql(table='testing', column=['word_col', 'int_col', 'int_col2'], col_type=['varchar', 'int', 'int'], line_ending="\n", rows=2048):
    line = ""
    line = line + "insert into " + table + \
        " (" + ", ".join(map(str, column)) + ") VALUES "
    for i in range(rows):
        line = line + "("
        for v in col_type:
            if v == 'varchar':
                line = line + "'" + random_sentence() + "', "
            elif v == 'int':
                line = line + "" + str(random.randint(1, 10000)) + ", "
        line = line.rstrip(', ')
        if i == rows - 1:
            line = line + ");" + line_ending
        else:
            line = line + "), " + line_ending
    return line

def generate_no_json(table='testing', column=['word_col', 'int_col', 'int_col2'], col_type=['varchar', 'int', 'int'], line_ending="\n", rows=2048):
    line = ""
    line = line + "insert into " + table + \
        " (" + ", ".join(map(str, column)) + ") VALUES "
    for i in range(rows):
        line = line + "("
        for v in col_type:
            if v == 'varchar':
                line = line + "'" + random_sentence() + "', "
            elif v == 'int':
                line = line + "" + str(random.randint(1, 10000)) + ", "
        line = line.rstrip(', ')
        if i == rows - 1:
            line = line + ");" + line_ending
        else:
            line = line + "), " + line_ending
    return line



def random_sentence(i=3):
    global wordlist
    sentence = ""
    for ii in range(i):
        sentence = sentence + random.choice(wordlist) + ' '
    return sentence.strip()


print("generating sql")
wf = open('sql/insert.sql', 'wb')
wf.write(generate_sql(rows=10000).encode('utf-8'))
wf.close()


print("generating nosql")
wf_no_sql = open('nosql/data.json', 'wb')
wf_no_sql.write(generate_sql(rows=10000).encode('utf-8'))
wf_no_sql.close()
f.close
