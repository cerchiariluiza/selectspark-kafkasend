
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import sum,avg,max,min,mean,count
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Create DataFrame dadoscadastrais with columns name,dept & age
dadoscadastrais = [("3170", "123", "1", "Joao da Silva"), ("9999","234","11155560688800001", "Marcio"), \
    ("3270","144","1", "Joao da Silva")]

dadoscadastraiscolunas= ["agencia","conta", "cnpj", "nome"]


tabledadoscadastrais = spark.createDataFrame(data = dadoscadastrais, schema = dadoscadastraiscolunas)

print("imprimindo esquema da dados cadastrais")
tabledadoscadastrais.printSchema()
print("imprimindo conteudo da dados cadastrais")
tabledadoscadastrais.show()

#Create DataFrame dadoscadastrais with columns name,dep,state & salary
boletos=[("3170","123","13","20/01/2002"),("3170","144","333","20/01/2002"), \
    ("9999","234","777","20/01/2002")]

boletoscolumnas= ["agencia","conta", "valor","data"]
dadosdaconta = spark.createDataFrame(data = boletos, schema = boletoscolumnas)
print("imprimindo esquema da tabvela boletos")
tabledadoscadastrais.printSchema()

print("imprimindo conteudo da tabela boletos")
tabledadoscadastrais.show()

#Add missing columns 'state' & 'salary' to dadoscadastrais
from pyspark.sql.functions import lit
dadoscadastrais.join(dadosdaconta,dadoscadastrais.agencia ==  dadosdaconta.agencia,"inner") \
     .show(truncate=False)
