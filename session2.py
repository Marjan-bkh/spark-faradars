from pyspark import SparkConf,SparkContext

#create a spark context
conf= SparkConf.setMaster("local").setAppName("session2")
sc= SparkContext(conf=conf)

#read the csv file using sc.textfile
rdd= sc.textFile("session2")


#skip the header
header= rdd.first()
rdd= rdd.filter (lambda row: row!= header)

#creating a new rdd that contains flower type
split= rdd.map(lambda x:x.split(','))
KeyValue = split.map(lambda x: (x[4].strip(),float(x[0])))

#Filtered rdd
Filtered= KeyValue.filter(lambda x:'Setosa' in x[o] or 'Versicolor' in x[0])

#if you want to see the outcome of this step:
F= Filtered.collect()
for i in F:
    print(i)

