import random
import json
wordlist = []

with open('word_list.txt', 'r') as f:
    for line in f.readlines():
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


def generate_nosql(table='testing', column=['word_col', 'int_col', 'int_col2'], col_type=['varchar', 'int', 'int'], line_ending="\n", rows=2048):
    lines = []
    for i in range(rows):
        line ={
            '_id': i,
        }
        for i, c in enumerate(column):
            if col_type[i] == 'varchar':
                line[c] = random_sentence().encode('utf-8')
            elif col_type[i] == 'int':
                line[c] = random.randint(1, 10000)
        lines.append(line)
    return lines


def random_sentence(i=3):
    global wordlist
    sentence = ""
    for ii in range(i):
        sentence = sentence + random.choice(wordlist) + ' '
    return sentence.strip()

if __name__ == "__main__":
    print("generating sql...")
    with open('sql/insert.sql', 'wb') as outfile:
        outfile.write(generate_sql(rows=10000).encode('utf-8'))


    print("generating nosql")
    with open('nosql/data.json', 'wb') as outfile:
        json.dump(generate_nosql(rows=10000), outfile)


