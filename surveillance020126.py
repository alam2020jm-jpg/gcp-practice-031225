class suerveillance020126:
    def __init__(self,spark,input_path):
        self.spark=spark 
        self.input_path=input_path
        if not spark.catalog.tableExists("main020126"):
            print(f"Spar has started the reading raw data from location:{input_path}")
            df=spark.read.format("csv").option("mode","permissive").option("header","true").option("inferSchema","true").load(input_path)
        else:
            print("Spark has already read the data.")
class layering(suerveillance020126):
    def logiclaying(self):
       """ df_new_order=self.df.filter(col("status")=='New')
        df_new_order=df_new_order.withColumn("eventinUnix",cast(col(EventTime).UNIX_TIMESTAMP()))
        window_specification=Window.partitionBy("AccountId","system").orderBy(col(EventTime).rangebetween(-60,0)
        logic_df=df_new_order.groupBy("AccountId","system").agg(count(order)).alias("OderCount")
        df_with5=logic_df.filter(col(OderCount)>=5) """
        # First APPROACH using Join
        df1=self.df.filter(col('status')=='New').withColumnRenamed("EventTime","New_event_time")
        df2=self.df.filter(col("status")=="cancel").withColumnRenamed("EventTime","Cancel_event_time")
        df_com=df1.join(df2,how='inner',on=(df1.OrderId==df2.OrderId))
        df_com=df_com.withcolumn("NewEventUnixTime",UNIX_TIMESTAMP_timestamp(col("New_event_time")))\
                .withcolumn(UNIX_TIMESTAMP(col("CancelEventUnixTime")))
        df_com_30=df_com.filter((CancelEventUnixTime-NewEventUnixTime)<30)
        window_specification=Window.partitionBy('symbol').orderBy(NewEventUnixTime).rangeBetween(-60,0)
        df_60_count=df_com_30.withColumn("ordercount",count(distinct OrderId).over(window_specification))
        return df_60_count.filter(col("ordercount")>5)
        # Second APPROACH Using Lead To Take The Order Cancel Time
        df2=self.df.withColumn("Event_unix",UNIX_TIMESTAMP(col("event_time")))
        window_spec_21=Window.partionBy(col("orderId"),(col("status")=="cancel")).orderBy(col("Event_unix")))
        df2_cancel_event=df2.withColumn("Cancel_unix",lead(col("Event_unix"),1).over(window_spec_21))
        df2_cance_in_30=df2_cancel_event.filetr((col("Event_unix")-col("Cancel_unix"))<30)
        window_spec_22=Window.partitionBy(col("orderId")).orderBy(col("Event_unix")).range(-60,0)
        df2_final=df2_cance_in_30,withColumn("layering_event_count",count(distince orderId).over(window_spec_22))
        return df2_final.filter(col("layering_event_count")>5)
        # THIRD APPROACH when
        df3= self.df
        df3_up=df3.groupBy("orderId")\
            .agg(
            min(when(clo("ststus")=="New",col("event_time")).alias("Event_Start_time")),
            sum(when(col("status")=="New",col('qty').otherwise(0))).alias("Total_order_qty"),
            max(when(col("status")=="cancel",col("eveent_time")).alias("Event_Last_Cancel_time")),
           sum(when(col("status")=="cancel",col(qty).otherwise(0)).alias("OrderwiseCancelledqty"))
           )
        df3_within30=df3_up.filter((UNIX_TIMESTAMP(col("Event_Start_time"))-UNIX_TIMESTAMP(col("Event_Last_Cancel_time"))),30)
    class pricemanupulation(suerveillance020126):
        def logicPriceManupulation(self):
            df_price=self.df.filter(date_format(col("Event_time"),"%H:%M:%S") Between ("15:15:00","15:30:00"))
            window_spe=Window.partitionBy(col("orderId")).orderBy(col("Event_time")).rowsBetween(Window.unboundedPreceeding,Window.unboundedFolowing)
            df_price=df_price.withColumn("Product_wise_Avg_price",avg(price).over(window_spe))
            df_price=df_price.filter((col("price")<.095* col("Product_wise_Avg_price") | col("price")>1.05*col("Product_wise_Avg_price") ))
            return df_price


