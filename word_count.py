import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.build.master('local').appName('WordCount').getOrCreate()
sc = spark.sparkContext

text_file = sc.testFile('https://raw.githubusercontent.com/Mina-ALLA/word_count/main/filesample.txt')
counts = text_file.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)

output = counts.collect()
for(word, count) in output :
    print('%s:%i' % (word, count)) 
