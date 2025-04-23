from pyspark import SparkConf, SparkContext

#create a Spark context
conf= SparkConf().setMaster("local").setAppName("session1")
sc= SparkContext(conf=conf)

#read the csv file using sc.textFile
rdd= sc.textFile("iris.csv")

# collect all line from the RDD
lines= rdd.collect()

#Display all components of the file
for line in lines:
    components = line.split(",")
    print(components)
