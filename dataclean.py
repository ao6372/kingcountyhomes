import pyspark as ps
import urllib.request
from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, split
import pyspark.sql.functions as F

spark = ps.sql.SparkSession.builder \
            .master("local[4]") \
            .appName("case study") \
            .getOrCreate()

sc = spark.sparkContext

def clean_real_prop_account(path='rawdata/real_property_account.csv'):
    #extracts zipcodes
    df=spark.read.csv(path, header=True)

    df.registerTempTable("prop_account")
    query="SELECT * from prop_account WHERE CityState LIKE '%WA'"
    df=spark.sql(query)

    colkey=df.select('ZipCode', concat(col("Major"), col("Minor")))

    #make pin column to join df together and match with zipcode
    colkey1=colkey.withColumnRenamed(existing='concat(Major, Minor)', new='pin')
    colkey2=colkey1.withColumn('Zip',rtrim(colkey1['ZipCode']))
    colkey3=colkey2.select('Zip','pin')
    finaldf=colkey3.withColumn('Zip', colkey3.Zip.cast('int'))
    return finaldf

def clean_res_building(path='rawdata/res_building.csv'):
    import pyspark.sql.functions as F
    df1=spark.read.csv(path, header=True)
    colkey=df1.withColumn('Zip', F.rtrim(df1['ZipCode']))

    #make pin column to join df together and match with zipcode
    colkey1=colkey.withColumn('pin',concat(col("Major"), col("Minor")))
    dffinal=colkey1.select(['pin','Zip','NbrLivingUnits','SqFtTotLiving','YrBuilt','Bedrooms', 'BathFullCount'])
    dffinal1=dffinal.withColumn("NbrLivingUnits_n", dffinal.NbrLivingUnits.cast("int"))
    dffinal2=dffinal1.withColumn("SqFtTotLiving", dffinal.SqFtTotLiving.cast("int"))
    dffinal3=dffinal2.withColumn("YrBuilt", dffinal.YrBuilt.cast("int"))
    dffinal4=dffinal3.withColumn("Bedrooms", dffinal.Bedrooms.cast("int"))
    dffinal5=dffinal4.withColumn("BathFullCount", dffinal.BathFullCount.cast("int"))
    return dffinal5

def clean_sales_data(path='rawdata/real_property_sales.csv'):
    df = spark.read.csv(path,
                             header=True,
                             quote='"',
                             sep=",",
                             inferSchema=True)

    df1=df.withColumn("pin", concat(df.Major, df.Minor))
    #df2=df1.withColumn('Month', split(df.DocumentDate, '/').alias('date').getItem(0).cast("int"))
    #df2=df2.withColumn('Day', split(df.DocumentDate, '/').alias('date').getItem(1).cast("int"))
    df2=df1.withColumn('Year', split(df.DocumentDate, '/').alias('date').getItem(2).cast("int"))


    df3=df2.select(['pin','Year','SalePrice'])
    df4=df3.filter((df3.Year>=2005) & (df3.Year<=2015))
    df5=df4.filter(df4.SalePrice>=20000)

    return df5
