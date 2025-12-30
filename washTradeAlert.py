class Tradesevillance:
    def __init__(self,spark,input_path):
        self.spark=spark
        self.input_path=input_path 
    if not spark.catelog.tableExists("RawData"):
        print("EWe rae going to read the data frame into spark")
        df=spark.read.format("csv").format("header",'true').option("inferSchema","true").load(self.input_data)
        df.createOrReplaceTempView("RawData")
    else:
        print("We have already read the data")
class washTradeAlert(Tradesevillance):
    def detectwashTrade(self):
        quey=self.spark.sql("""
        SELECT *,LAG(BUYER_ID,1) OVER(PARTITION BY Symbol ORDER BY TradeTime ) AS "previousBuyer",
        LAG(TradeTime,1) OVER(PARTITION BY Symbol ORDER BY TradeTime) AS "PreviousTimePurchase"
        FROM RawData 
        WHERE BUYER_ID =previousBuyer AND DATEDIFF(TradeTime,PreviousTimePurchase) <60
        """")
    return self.spark.sql(query)
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("DetectwashTrad").getOrCreate()
input_path="gs://scb-trading-bucket/raw_trades.csv"
out_path="gs://scb-trading-bucket/procedded_trades.json"
first_wash_object=washTradeAlert(spark,input_path)    
final_deted_data=first_wash_object.detectwashTrade()
final_deted_data.write.format("JSON").save("out_path")