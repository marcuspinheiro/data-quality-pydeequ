# Databricks notebook source
#maven 
#com.amazon.deequ:deequ:2.0.6-spark-3.4

# COMMAND ----------

pip install pydeequ

# COMMAND ----------

import os

# COMMAND ----------

os.environ["SPARK_VERSION"] = "3.3"

# COMMAND ----------

from pyspark.sql import SparkSession, Row
import pydeequ

spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/gym-demo/tables/users_hist/")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Completeness: verifica o quão completo está uma coluna, ou seja, a quantidade de valores não nulos.
# MAGIC
# MAGIC Mean: realiza uma média dos valores de uma coluna, é um analyzer estatístico para os datasets.
# MAGIC
# MAGIC Size: verifica o tamanho total de linhas considerando o dataset como um todo.

# COMMAND ----------

from pyspark.sql import SparkSession
from pydeequ.analyzers import AnalysisRunner, Size, Completeness

# COMMAND ----------

from pydeequ.analyzers import *

analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("cpf")) \
                    .addAnalyzer(Completeness("name")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Constraint Suggestions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC isUnique: verifica se uma determinada coluna possui apenas valores únicos.
# MAGIC
# MAGIC hasMin: verifica se todos os valores de uma coluna são menos que um valor estabelecido para os testes.
# MAGIC
# MAGIC hasMax: verifica se todos os valores de uma coluna são maiores que um valor estabelecido para os testes.
# MAGIC
# MAGIC isContainedIn: verifica se todos os valores de uma coluna estão dentro de um conjunto de valores estabelecidos para os testes.

# COMMAND ----------

from pydeequ.suggestions import *

suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()

# Constraint Suggestions in JSON format
print(suggestionResult)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constraint Verification

# COMMAND ----------

check_level_error = CheckLevel.Error

# COMMAND ----------

from pydeequ.checks import *
from pydeequ.verification import *

check = Check(spark, CheckLevel.Error, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.isComplete("cpf")  \
        .isUnique("name")  \
        .isContainedIn("age", ["35", "40", "98"])) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pydeequ.analyzers import AnalysisRunner, Completeness
from pydeequ.checks import Check, CheckLevel

# COMMAND ----------

# MAGIC %md
# MAGIC isUnique: verifica se uma determinada coluna possui apenas valores únicos.
# MAGIC
# MAGIC hasMin: verifica se todos os valores de uma coluna são menos que um valor estabelecido para os testes.
# MAGIC
# MAGIC hasMax: verifica se todos os valores de uma coluna são maiores que um valor estabelecido para os testes.
# MAGIC
# MAGIC isContainedIn: verifica se todos os valores de uma coluna estão dentro de um conjunto de valores estabelecidos para os testes.
