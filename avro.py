import pyspark.sql.functions as fcn
import pandas as pd
from pyspark.sql.types import *
import json

from calendar import month_name
from io import BytesIO
import datetime
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.context import SparkContext
import sys
sc = SparkContext('local')
spark = SparkSession(sc)
schema = StructType(
    [
        StructField("_id",StringType(),True),
        StructField("Id",LongType(),True),
        StructField("Timestamp",StructType(
            [
                StructField("$date",LongType(),True)
            ]
        ), True),
        StructField("TTNR",StringType(),True),
        StructField("SNR",LongType(),True),
        StructField("State",LongType(),True),
        StructField("I_A1",StringType(),True),
        StructField("I_B1",StringType(),True),
        StructField("I1",DoubleType(),True),
        StructField("Mabs",DoubleType(),True),
        StructField("p_HD1",DoubleType(),True),
        StructField("pG",DoubleType(),True),
        StructField("pT",DoubleType(),True),
        StructField("pXZ",DoubleType(),True),
        StructField("T3",DoubleType(),True),
        StructField("nan",DoubleType(),True),
        StructField("Q1",LongType(),True),
        StructField("a_01X",ArrayType(DoubleType()),True)
      ]
)

example = {"_id": '{"$oid": "5eb56a371af2d82e242d24ae"}',"Id": 7,"Timestamp": {"$date": 1582889068586},"TTNR": "R902170286","SNR": 92177446,"State": 0,"I_A1": "FALSE","I_B1": "FALSE","I1": 0.0037385,"Mabs": -20.3711051,"p_HD1": 30.9632005,"pG": 27.788934,"pT": 1.7267373,"pXZ": 3.4487671,"T3": 25.2357555,"nan": 202.1999969,"Q1": 0,"a_01X": [62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925,62.7839925]}

data_stream_value = spark.createDataFrame(
    pd.DataFrame(
    {
      'foo': 'bar',
      'value': json.dumps(example)
    }, index=[0]
  )
)
jsonDF = data_stream_value.select(fcn.from_json(data_stream_value.value, schema).alias("json_detail"))
jsonDF = jsonDF.select("json_detail.*")
jsonDF.show()
