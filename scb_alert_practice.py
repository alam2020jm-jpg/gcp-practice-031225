from pyspar.sql import SparkSession 
input="gs://bucket/file_name"
spark=SparkSession.builder.appName("Alert").getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(input_path)
df_delta=df.write.format("delta").save(out_put)
create or replace Temp Scb_data as 
SELECT * FROM out_put
class Tradesevillance:
    def __init__(self,spark,input_spark):
        self.spark=spark
        self.input_spark=input_spark
        if not spark.catelog.tableExist("MainRawData"):
            print("Reading the raw data in the spark is strated")
            self.df=spark.read.format("csv").format("header","true").option("inferSchema","true").load(self.input_spark)
            self.df.createOrReplace TempView("MainRawData")
        else:
            print("The TemView already created ,No need ")
class HighValueTrade(Tradesevillance):
    def higValueAlert(self):
        query=spark.sql("""
        SELECT trade_id,tradeType,price,qty,tradeTime(price*qty) as TradeVolume,"HighAlert" from MainRawData
        where TradeVolume>1000000
        """)
    return self.spark.sql(query)
class HighPriceAlert(Tradesevillance):
    def highPriceAlert(self):
        query=spark.sql("""
        SELECT trade_id,tradeType,price,qty,tradeTime,
        lag(price,1) over(partition by tradeType order bytradeTime) as SucpeciousPrice from MainRawData
        """)
    return self.spark.sql(query)
spark=SparkSession.bulider.appName("TradeSurveillance").getOrCreate()
input_path="gs://my_bucket/myrawfile.csv"
first_surveillance=Tradesevillance(spark,input_path)
highValObject=HighValueTrade(spark,input_path)
highValObject.higValueAlert()
highPriceObject=HighPriceAlert(spark,input_path)
highPriceObject.highPriceAlert()



