class Tradesurveillance:
    def __init__(self,spark,input_path):
        self.spark=spark
        self.input_path=input_path
        if not self.spark.catalog.tableExists("MainRawData311225"):
            print("Spark has started the reading data from GCS BUCKETS")
            df=self.spark.read.format("csv").option("header","true").option("inferSchema","true").load(self.input_path)
            df.createOrReplaceTempView("MainRawData311225")
        else:
            print("Data has been already read by Spark , we dont required to read again")
class circularTradeAlert(Tradesurveillance):
    def ciralert(self):
        query=""" with final as(
        SELECT *,LAG(BuyerID,1) OVER(PARTITION BY Symbol ORDER BY TradeDate ) AS previus_buyer,
        LAG(BuyerID,2) OVER(PARTITION BY Symbol ORDER BY TradeDate) AS previus_buyer1,
        LAG(SellerID,1) OVER(PARTITION BY Symbol ORDER BY TradeDate) AS previous_seller,
        LAG(TradeDate,1) OVER(ORDER BY TradeDate) AS previous_date
        FROM  MainRawData311225)
        select * from final 
        WHERE BuyerID=previous_seller AND SellerID=previus_buyer1 AND 
        DATEDIFF(TradeDate, previous_date)<=1 """
        #sql_quiry=self.spark.sql("""query""")
        return self.spark.sql(query)
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Survaillance").getOrCreate()
path="gs://my_bucket/data.csv"
PROCEDD_PATH="gs://bucket/proccesed.json"
crea_obj=circularTradeAlert(spark,path)
procedd_data=crea_obj.ciralert()
print("Writing the processed data into Json fomrate")
procedd_data.write.format("json").option("mode","append").save(PROCEDD_PATH)
print(f"Proccesd Data Sucessfully saved into GSC at loaction :- {PROCEDD_PATH}")

