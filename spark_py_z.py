from pyspark.sql import Row
from pyspark.sql.functions import desc
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df_prod = spark.createDataFrame([
    Row(prod='sosigeA', id_prod=1),
    Row(prod='waterA', id_prod=2),
    Row(prod='apple', id_prod=3),
    Row(prod='banana', id_prod=4),
    Row(prod='tualetka', id_prod=0),
])

df_categor = spark.createDataFrame([
    Row(categ='meet', id_categ=1),
    Row(categ='water', id_categ=2),
    Row(categ='frut', id_categ=3),
    Row(categ='eat', id_categ=4),
    Row(categ='NULL', id_categ=0),
])


df_conect = spark.createDataFrame([
    Row(id_prod = 1, id_categ=1),
    Row(id_prod = 1, id_categ=4),
    Row(id_prod = 2, id_categ=2),
    Row(id_prod = 3, id_categ=3),
    Row(id_prod = 4, id_categ=3),
    Row(id_prod = 0, id_categ=0),
])


df_prod.join(df_conect).where(df_prod["id_prod"] == df_conect["id_prod"]) \
    .join(df_categor).where(df_categor["id_categ"] == df_conect["id_categ"]) \
        .select(df_prod["prod"],df_categor["categ"]).show()