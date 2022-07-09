/*

Aula 04

Exercícios - Dataset com DataFrame 

spark-shell

scala

*/



import org.apache.spark.sql.types._

//Dataframe

val columns_list = List(
    StructField("nome", StringType),
    StructField("sexo", StringType),
    StructField("quantidade", IntegerType)
)

val names_us_schema = StructType(columns_list)

val names_us = spark.read.schema(names_us_schema).csv("/user/daniel/data/exercises-data/names/yob*")

names_us.printSchema()

names_us.take(5)

names_us.show(5)


//dataset

case class Nascimento(nome: String, sexo: String, quantidade:Integer)

val NascDS = names_us.as[Nascimento]

NascDS.printSchema()

NascDS.show(5)

NascDS.write.option("compression", "snappy").parquet("/user/daniel/names_us_parquet")



//2

case class Nascimento(nome: String, sexo: String, quantidade:Integer)

import org.apache.spark.sql.Encoders

val schema_name = Encoders.product[Nascimento].schema

val name_ds = spark.read.schema(schema_name).csv("/user/daniel/data/exercises-data/names").as[Nascimento]

name_ds.printSchema()

name_ds.show(5)

name_ds.write.save("/user/daniel/names_us_parquet")

spark.read.parquet("/user/daniel/names_us_parquet").printSchema()


