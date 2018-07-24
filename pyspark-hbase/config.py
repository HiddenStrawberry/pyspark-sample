# -*- coding: utf-8 -*-
import os
from utils import get_hbase_params

HBASE_DIR = os.environ.get("HBASE_CONF", '')
if HBASE_DIR:
	res = get_hbase_params("%s/hbase-site.xml", ['hbase.zookeeper.quorum'])
	CONF = {
		"hbase.zookeeper.quorum": res['hbase.zookeeper.quorum']['value']
	}
else:
	CONF = {}

KEYCLASS = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
VALUECLASS = "org.apache.hadoop.io.Writable"
REVALUECLASS = "org.apache.hadoop.hbase.client.Result"

INPUTFORMATCLASS = "org.apache.hadoop.hbase.mapreduce.TableInputFormat"
INKEYCONV = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
INVALUECONV = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

OUTPUTFORMATCLASS = "org.apache.hadoop.hbase.mapreduce.TableOutputFormat"
OUTKEYCONV = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
OUTVALUECONV = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
