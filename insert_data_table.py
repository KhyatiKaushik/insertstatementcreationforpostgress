from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
import datetime


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)


def standardize_nulls(df):
    """
    Function to standardize all nulls across the data frame
    :param df: Input Data Frame
    :return: Standardized Data Frame
    """
    for column in df.columns:
        if isinstance(df.schema[column].dataType, IntegerType) or \
                isinstance(df.schema[column].dataType, LongType) or \
                isinstance(df.schema[column].dataType, DoubleType) or \
                isinstance(df.schema[column].dataType, ByteType) or \
                isinstance(df.schema[column].dataType, ShortType) or \
                isinstance(df.schema[column].dataType, FloatType) or \
                isinstance(df.schema[column].dataType, DecimalType):
            df = df.fillna(0, subset=[column])

        if isinstance(df.schema[column].dataType, TimestampType) or isinstance(df.schema[column].dataType, DateType):
            df = df.withColumn(column,
                               F.when(F.col(column).isNull(), F.lit('')).otherwise(
                                   F.regexp_replace(F.date_format(F.col(column), "yyyy-MM-dd\tHH:mm:ss"), "\t", "T")))

        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column,
                               F.when(F.col(column).isNull(), F.lit('')).otherwise(F.col(column)))

            df = df.withColumn(column,
                               F.when(F.col(column).isin("NULL", "NA", "NaN", "", "None"), F.lit("")).otherwise(F.col(column)))

        if isinstance(df.schema[column].dataType, StructType):
            df = df.withColumn(column,
                               F.when(df[column].isNull(), F.coalesce(
                                   F.col(column),
                                   F.from_json(F.lit("{}"),
                                               df.schema[
                                                   column].simpleString().split(
                                                   column + ":")[1])
                               )).otherwise(df[column]))

        if isinstance(df.schema[column].dataType, ArrayType):
            df = df.withColumn(column,
                               F.when(df[column].isNull(), F.coalesce(
                                   F.col(column),
                                   F.from_json(F.lit("[]"),
                                               df.schema[
                                                   column].simpleString().split(
                                                   column + ":")[1])
                               )).otherwise(df[column]))

    return df


def read_csv(file_path):
    """
    Function to read a csv file
    :param file_path: Input file path
    :return: Spark data frame
    """
    try:
        df_csv = spark.read.csv(file_path, header=True, inferSchema=False, escape="\"", quote="\"")

        # Renaming all column names to lowercase
        for col in df_csv.columns:
            df_csv = df_csv.withColumnRenamed(col, col.lower())
    except AnalysisException as ex:
        df_csv = None

    return df_csv


def read_parquet(file_path):
    """
    Function to read data in parquet files
    :param file_path: Input file path
    :return: Spark data frame
    """
    try:
        df = spark.read.parquet(file_path)

        # Renaming all column names to lowercase
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower())
    except AnalysisException as ex:
        df = None

    return df

filepath_path = 'C;/XXXX/yiiir.csv'
df_filepath = read_csv(filepath_path)

# print(df_org.printSchema())
df_filepath = standardize_nulls(df_filepath)
columns = "INSERT INTO tablename("
for col in df_filepath.columns:
    columns = columns + col + ","
columns = columns[:-1] + ")"
# print(columns)
rows = []
for row in df_filepath.collect():
    stmt = columns + " VALUES("+str(row["colname1"])+", '"+str(row["colname12"])+"', " \
            "'"+str(row["colname3"])+"', '"+str(row["colname4"])+"', '"+str(row["colname5"])+"', '"+str(row["colname6"])+"', " \
            "'"+str(row["colname7"])+"', '"+str(row["colname8"])+"', '"+str(row["colname9"])+"', "+str(row["colname10"])+");"
    rows.append(stmt)
#     print(stmt)

with open('filename.txt', 'w') as f:
    f.write('\n'.join(rows))
