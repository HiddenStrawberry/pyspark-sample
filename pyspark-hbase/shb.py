# -*- coding: utf-8 -*-
from config import *
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import json
import datetime

TYPEDICT = {
    "string": StringType(),
    "boolean": BooleanType(),
    "float": FloatType(),
    "int": IntegerType(),
    "long": LongType(),
    "date": DateType(),
    "datetime": TimestampType()
}


def value_type_transfer(v, type):
    value = None
    if v:
        try:
            if type == 'string':
                value = v
            elif type == 'boolean':
                value = bool(v)
            elif type == 'float':
                value = float(v)
            elif type == 'int':
                value = int(v)
            elif type == 'long':
                value = long(v)
            elif type == 'date':
                value = datetime.datetime.strptime(v, '%Y-%m-%d')
            elif type == 'datetime':
                value = datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
            else:
                value = v
        except:
            value = None
    return value


def transferString(v, type):
    value = ''
    if not v:
        value = ''
    elif type == 'boolean':
        value = v and 'True' or 'False'
    elif type == 'date':
        value = v.strftime("%Y-%m-%d")
    elif type == 'datetime':
        value = v.strftime("%Y-%m-%d %H:%M:%S")
    else:
        value = '%s' % v
    return value


class shb(object):
    def __init__(self, sc, sqlContext, conf=CONF):
        self.conf = conf
        self.prefix = conf.get('prefix', '')
        self.sc = sc
        self.sqlContext = sqlContext
        self.cache_rdd = {}

    def get_df_from_hbase(self, catelog, cached=True):
        """
		:param catelog: json , eg:
		{
			"table":{"namespace":"default", "name":"table1"},
			"rowkey":"col0",
			"domain": "id > 1 and skuid = 124 "
			"columns":{
			  "col0":{"cf":"rowkey", "col":"key", "type":"string"},
			  "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
			  "col3":{"cf":"cf3", "col":"col3", "type":"float"},
			  "col4":{"cf":"cf4", "col":"col4", "type":"int"},
			  "col5":{"cf":"cf5", "col":"col5", "type":"long"},
			  "col7":{"cf":"cf7", "col":"col7", "type":"date"},
			  "col8":{"cf":"cf8", "col":"col8", "type":"datetime"}
			}
		}
		:return: :class:`DataFrame`
		"""
        conf = self.conf.copy()
        prefix = catelog['table']['namespace'] == "default" and self.prefix or catelog['table']['namespace']
        table = catelog['table']['name']
        conf['hbase.mapreduce.inputtable'] = prefix and "%s_%s" % (prefix, table) or table
        hbase_rdd = self.sc.newAPIHadoopRDD(INPUTFORMATCLASS, KEYCLASS, VALUECLASS, keyConverter=INKEYCONV,
                                            valueConverter=INVALUECONV, conf=conf)

        def hrdd_to_rdd(rdds):
            new_rdds = []
            for index, rdd in enumerate(rdds):
                values = rdd[1].split("\n")
                new_value = {}
                for x in values:
                    y = json.loads(x)
                    new_value["%s:%s" % (y['columnFamily'], y['qualifier'])] = y['value']
                new_rdd = {}
                for column, v in catelog['columns'].items():
                    real_v = v['cf'] == 'rowkey' and rdd[0] or new_value.get("%s:%s" % (v['cf'], v['col']), None)
                    new_rdd[column] = value_type_transfer(real_v, v['type'])
                new_rdds.append(new_rdd)
            return new_rdds

        rdds = hbase_rdd.mapPartitions(hrdd_to_rdd)

        if cached:
            rdds.cache()
            self.cache_rdd[table] = rdds

        df = self.sqlContext.createDataFrame(rdds, self._catelog_to_schema(catelog))
        if catelog.get('domain', ''):
            df = df.filter(catelog['domain'])
        return df

    def uncache_rdd(self, tablename):
        if self.cache_rdd.has_key(tablename):
            self.cache_rdd[tablename].unpersist()
        return True

    def save_df_to_hbase(self, dataframe, catelog):
        """
		:param dataframe: :class:`DataFrame`
		:param catelog: eg: :method:`get_df_from_hbase`
		:return:
		"""

        def f(rdds):
            columns = catelog["columns"]
            rowkey = catelog["rowkey"]
            newrdds = []
            for rdd in rdds:
                rdd_dict = rdd.asDict()
                key = str(rdd_dict[rowkey])
                for k, v in rdd_dict.items():
                    if columns.has_key(k) and k != rowkey:
                        newrdds.append(
                            (key, [key, columns[k]['cf'], columns[k]['col'], transferString(v, columns[k]['type'])]))
            return newrdds

        conf = self.conf.copy()
        prefix = catelog['table']['namespace'] == "default" and self.prefix or catelog['table']['namespace']
        table = catelog['table']['name']
        conf.update({
            "mapreduce.outputformat.class": OUTPUTFORMATCLASS,
            "mapreduce.job.output.key.class": KEYCLASS,
            "mapreduce.job.output.value.class": VALUECLASS
        })
        conf['hbase.mapred.outputtable'] = prefix and "%s_%s" % (prefix, table) or table
        dataframe.rdd.mapPartitions(f).saveAsNewAPIHadoopDataset(conf=conf, keyConverter=OUTKEYCONV,
                                                                 valueConverter=OUTVALUECONV)
        return True

    def _catelog_to_schema(self, catelog):
        columns_dict = catelog.get('columns')
        columns = columns_dict.keys()
        structtypelist = [StructField(x, TYPEDICT.get(columns_dict[x]['type'], StringType()), True) for x in columns]
        schema = StructType(structtypelist)
        return schema


if __name__ == '__main__':
    from pyspark import SparkConf, SparkContext

    conf = SparkConf().setAppName("pyspark-hbase")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    tests = shb(sc, sqlContext)
    catelog = {
        # "table": {"namespace": "default", "name": "nt_prod"},
        "table": {"namespace": "default", "name": "emp"},
        "rowkey": "id",
        "domain": "",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "skuid": {"cf": "info", "col": "SKUID", "type": "int"},
            "c_store_id": {"cf": "info", "col": "C_STORE_ID", "type": "int"}
        }
    }

    df = sqlContext.createDataFrame(
        [{'id': '2', 'skuid': 12, 'c_store_id': 1}, {'id': '3', 'skuid': 4, 'c_store_id': None}])
    tests.save_df_to_hbase(df, catelog)
    df = tests.get_df_from_hbase(catelog)
    df.show(10)
