### Python script to extract strings from thousands of CSV files (downloaded from JSTOR's data for research feature) 
### and insert them into a mySQL database

### This code takes IDs from a file and composes lists of the ngrams (words and phrases) from the papers corresponding 
### to those IDs. 
### The list also includes the number of times each ngram was used and author name and gender
### It then executes mySQL commands through pymysql to insert the ngrams into my database

from stemming.porter2 import stem
bigrams = []
trigrams = []
quadgrams = []
    
ids = []
### This CSV file contains the IDs for the files I want to extract data from
for line in open('singleauthor-id-name.csv',errors='ignore'):
    line = line.replace('/', '_').replace("\n",'')
    ids.append(line)

### This loop extracts bigrams, trigrams, and quadgrams from their respective files (several million strings in total)
for i in ids:
    file = 'bigrams/bigrams_'+i.split(',')[0]+'.CSV'
    for line in open(file,errors='ignore'):
        line = ' '.join([stem(x) for x in line.replace(',',' ,').split()])
        if line[0:6] == 'BIGRAM':
            del line
        else:
            bigrams.append(line+', '+i)
    file = 'trigrams/trigrams_'+i.split(',')[0]+'.CSV'
    for line in open(file,errors='ignore'):
        line = ' '.join([stem(x) for x in line.replace(',',' ,').split()])
        if line[0:7] == 'TRIGRAM':
            del line
        else:
            trigrams.append(line+', '+i)
    file = 'quadgrams/quadgrams_'+i.split(',')[0]+'.CSV'
    for line in open(file,errors='ignore'):
        line = ' '.join([stem(x) for x in line.replace(',',' ,').split()])
        if line[0:8] == 'QUADGRAM':
            del line
        else:
            quadgrams.append(line+', '+i)
    print(ids.index(i)+1)

### This executes mySQL code within Python. I use it to create tables and feed data into them
import pymysql.cursors
connection = pymysql.connect(host='localhost',
                             user='Stephen',
                             passwd='**********',
                             db='*********',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
counter = 0
### Some example SQL commands to execute
#sql = "CREATE TABLE bigrams(ngram varchar(300),count int, paper_id varchar(20),author varchar(200),gender int)"
#sql = "CREATE TABLE trigrams(ngram varchar(300),count int, paper_id varchar(20),author varchar(200),gender int)"
#sql = "CREATE TABLE quadgrams(ngram varchar(300),count int, paper_id varchar(20),author varchar(200),gender int)"
#sql = "INSERT INTO bigrams (ngram, count, paper_id, author, gender) VALUES (%s, %s, %s, %s, %s)"
#sql = "INSERT INTO trigrams (ngram, count, paper_id, author, gender) VALUES (%s, %s, %s, %s, %s)"
#sql = "INSERT INTO quadgrams (ngram, count, paper_id, author, gender) VALUES (%s, %s, %s, %s, %s)"
for x in quadgrams: ### or bigrams or trigrams
    insertngram = x.split(',')
    cursor.execute(sql,(insertngram[0].rstrip(),insertngram[1],insertngram[2].lstrip(),insertngram[3],insertngram[4],))
    if counter == 25000:
        connection.commit()
        counter = 0
    counter += 1
#cursor.execute(sql) ### For the "CREATE TABLE" commands
connection.commit()
