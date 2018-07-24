# encoding=utf8
import argparse


from pyspark import HiveContext, SparkConf, SparkContext, Row

'''
@需求：可配置的表处理（输入格式不唯一，输出格式唯一）
'''


def process_table(element):
    row_list = index.Config(element).get_row()
    return row_list

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Data to new Hive Table")
    parser.add_argument("-source", help="Source hive table name")
    parser.add_argument("-target", help="Target hive table name")
    parser.add_argument("-config_file", help="Config .py file")
    parser.add_argument("-db", help="Database name")
    args = parser.parse_args()

    if args.db:HIVE_DB=args.db

    index=__import__(args.config_file) #config file

    conf = SparkConf().setAppName("pyspark-hive")  # connect to spark
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)

    hc.sql('USE {0}'.format(HIVE_DB))
    df=hc.sql(args.source)
    df=df.flatMap(process_table)
    df=df.toDF()
    df=df.registerTempTable('temptable')
    hc.sql("create table {} as select * from temptable".format(args.target))

    '''
    备注：
    当没有Create权限时可以尝试借助Temptable写入
    '''



