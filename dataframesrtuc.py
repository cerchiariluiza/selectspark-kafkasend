import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "blog", table_name = "table_input", transformation_ctx = "datasource0")

# datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": "s3://imobiliario/pastaprincipal/parquet/"}, format = "parquet", transformation_ctx = "datasink2")
memberships = glueContext.create_dynamic_frame.from_catalog(database = "blog", table_name = "table_input")
memberships.toDF().createOrReplaceTempView("memberships")
busca = spark.sql("select * from memberships")
# print("tipo")
# print(type(busca))
# busca.printSchema()
# busca.show()



schema = StructType(
    [
    StructField('codigo_cnpj_utilizacao_bonus_relacionamento', StringType(), True),
    StructField('data_contratacao', StringType(), True),

    StructField('item_bonus_relacionamento', StructType(
        [

        StructField('codigo_tipo_bonus_relacionamento', StringType(), True), #codigo_modalidade
        StructField('nome_tipo_bonus_relacionamento', StringType(), True),
        StructField('detalhe_bonus_relacionamento', StructType(

            [
                StructField('agencia', StringType(), True),
                StructField('conta', StringType(), True),
                StructField('digito', StringType(), True),

            ]

            ), True),


        StructField('valor_total_detalhe_bonus_relacionamento', StringType(), True),    #valor_operacao
        
        ]), True),
 
])


from pprint import pprint

newJson = []
    


for message in busca.collect(): 

    # print(message['cnpj_empresa'])

    #só inserir no  json os dados que o penultimo pagamento for ==hoje 
    #só inserir no  json  se todos os "e_pagamento_nao_pactuado" == "false"
    newJson.append({
    "codigo_cnpj_utilizacao_bonus_relacionamento": f"{message['cnpj_empresa']}" ,
    "data_contratacao": f"{message['data_contratacao']}" ,
    "item_bonus_relacionamento": {

        "codigo_tipo_bonus_relacionamento": f"{message['nome_modalidade']}" ,
        "nome_tipo_bonus_relacionamento": f"{message['codigo_modalidade']}" ,
        "detalhe_bonus_relacionamento":{

            "agencia": f"{message['agencia_cliente']}" ,
            "conta": f"{message['conta_cliente']}" ,
            "digito": f"{message['digito_conta_cliente']}" ,
            "valor_bonus_relacionamento": f"{message['valor_operacao']}" ,

        },
        "valor_total_detalhe_bonus_relacionamento": f"{message['valor_operacao']}" ,
        }


    }

)






# pprint(newJson)

# Create data frame
df = spark.read.json(sc.parallelize(newJson), schema, multiLine=True)
# print(df.schema)
df.show()


# IN AWS CONSOLE UNCOMMENT, IT WIL PERSISTS IN DATABASE
#convert to dynamic frame, save it to the bucket, and everything in it is automatically saved to the table because it is mapped in aws (see ddl.sql)
from awsglue.dynamicframe import DynamicFrame
dif = DynamicFrame.fromDF(df, glueContext, "test_nest")
print(type(dif))
dif.show()

