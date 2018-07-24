import argparse

from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql.functions import isnull

'''
@需求：

处理来自HDFS的json文件，统计各个字段的空值百分比/数量

'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Data to new Hive Table")
    parser.add_argument("-path", help="input file path")
    parser.add_argument("-output", help="output file path")
    args = parser.parse_args()

    conf = SparkConf().setAppName("pyspark-json") #connect to spark
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    input_path=args.path         #'hdfs://nameservice1/user/hiddenstrawberry/test.json'
    df=sqlContext.read.json(input_path) #read json file
    columns=df.columns #columns list
    counts=df.count()
    dct={'count':{},'percent':{}}
    for each in columns:
        count = df.filter(isnull(each)).count() #null filter
        dct['count'][each]=count
        dct['percent'][each]=float(count)/float(counts)
    dct['namelist']=list(set([int(i.name) for i in df.collect()]))
    dct['totalcount']=counts

    #add code here to write dict to local or HDFS


