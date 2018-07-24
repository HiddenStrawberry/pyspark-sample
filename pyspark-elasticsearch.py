from pyspark import SQLContext, SparkConf, SparkContext


'''
@需求：

将来自HDFS的json文件，写入到Elasticsearch中
Documentation:https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
'''
if __name__ == '__main__':
    conf = SparkConf().setAppName("pyspark-elasticsearch") #connect to spark
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    input_path= 'hdfs://nameservice1/user/hiddenstrawberry/test.json'
    df=sqlContext.read.json(input_path)
    df.write.format("org.elasticsearch.spark.sql")\
        .option("es.nodes", "222.222.222.222") \
        .option("es.net.http.auth.user", "admin") \
        .option("es.net.http.auth.pass", "admin") \
        .option("es.port", 9200) \
        .option("es.nodes.wan.only","true") \
        .option("es.resource", "test1/doc") \
        .option("es.mapping.id", "@id") \
        .option("es.write.operation","upsert") \
        .mode("Overwrite").save()
    print df.count()

    '''
    es.nodes.wan.only参数可能在做了负载均衡的分布式es中必要
    es.resource:index/type
    es.mapping.id会指定一个字段为_id，即无需手动指定_id
    es.write.operation支持upsert操作
    Documentation:https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    '''
