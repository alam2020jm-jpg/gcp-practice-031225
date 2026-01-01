class parentsurveillance:
    def __init__(self,spark,input_path):
        self.spark=spark 
        self.input_path=input_path 
    if not spark.catalog.tableExists("MainTemp010126"):
        print(f"Spark has strated raeding data from loaction:-{input_path }")
        df=spark.read.format("csv")\
            .option("mode",'permissive')\
            .option("header",'true')\
            .option('inferSchema',"true")\
            .load(input_path)
        df.createOrReplaceTempView("MainTemp010126")
    else:
        print("Spark has already read the data, Now only we use the Temp View created")
class WashTrading(parentsurveillance):
    def logicWashTrade(self):
        quirywash=""" WITH FINAL AS (
        SELECT *,LAG(BuyerID,1) OVER(PARTITION BY Symbol ORDER BY TradeTime) as Previous_Buyer,
        LAG(SellerID,1) OVER(PARTITION BY Symbol ORDER BY TradeTime) as Previous_Seller 
        FROM MainTemp010126 }
        SELECT TradeId,Symbol,Price,Qrt,"WashTrader" AS AlertType
        FROM FINAL 
        WHERE BuyerId=Previous_Seller AND SellerID=Previous_Buyer 
        """"
        return self.spark.sql(quirywash)
class CircularTrading(parentsurveillance):
    def logicCircularTrade(self):
        querycircular=""" WITH FINALCIR AS (
        SELECT * ,
        LAG(BuyerId,1) OVER(PARTITION BY Symbol ORDER BY TradeTime) AS PreviousCirBuyer,
        LAG(BuyerId,2) OVER(PARTITION BY SYMBOL ORDER BY TradeTime) AS PreviouscirBuyer1,
        LEAD(SellerId,1) OVER(PARTITION BY SYMBOL ORDER BY TradeTime) AS PreviouscirSeller,
        LAG(SellerId,2) OVER(PARTITION BY SYMBOL ORDER BY TradeTime) AS PreviouscirSeller1
        FROM  MainTemp010126 )
        SELECT TradeId,Symbol,Price,Qrt,"CircularTrader" AS AlertType
        FROM FINALCIR 
        WHERE BuyerId=PreviouscirSeller AND SellerID=PreviouscirBuyer1 AND PreviousCirBuyer=PreviouscirSeller1
        """
        return self.spark.sql(querycircular)
class Spoofing(parentsurveillance):
    def logicSpoofingrTrade(self):
        querySpoofing="""
        SELECT TradeId,Symbol,Price,Qrt,"SpoonfigTrader" AS AlertType
        FROM MainTemp010126
        WHERE Event='CANCEL' AND UNIX_TIMEDIFF(OderTime,CancellTime)<=10 AND Oty>=100000 
        """
        return self.spark.sql(querySpoofing)
class FrontRunning(parentsurveillance):
    def logicFrontRunnerTrade(self):
        quiryFront="""
        SELECT TradeId,Symbol,Price,Qrt,"FrontRunnerTrader" AS AlertType
        FROM MainTemp010126
        WHERE EventType='RFQ' AND RFQ_ReceiverId=Buyer_ID AND RFQ_Symbol=Buyer_Symbol AND UNIX_TIMESTAMP(RFQ_Time,Buyer_Time)<=5
        """
        return self.spark.sql(quiryFront)
class InsiderTrading(parentsurveillance):
    def logicInsideTrade(self):
        queryInside="""
        SELECT TradeId,Symbol,Price,Qrt,"InsideTrader" AS AlertType
        FROM MainTemp010126
        WHERE qty<500000
        """"
        return self.spark.sql(queryInside)
class PumpandDump(parentsurveillance):
    def logicPumpAndDupmTrade(self):
        quiryPumpandDump=""" WITH FINALDUMP(
        SELECT *,
        AVG(Price)OVER(PARTITION BY SYMBOL ORDER BY TradeDate ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS Avg5DayPrice
        from MainTemp010126 )
        SELECT TradeId,Symbol,Price,Qrt,"PumpAndDumpTrader" AS AlertType
        FROM FINALDUMP  
        WHERE (Price/Avg5DayPrice)<0.7
        """
        return self.spark.sql(quiryPumpandDump)
