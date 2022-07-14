import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__=="__main__":
    
    spark=SparkSession.builder.master("local").appName("Weather Kafka Spark ").getOrCreate()
    
    sc=spark.sparkContext
    
    ssc=StreamingContext(sc,30)
    
    message=KafkaUtils.createDirectStream(ssc,topics=['WeatherTopic'],kafkaParams= {"metadata.broker.list": "localhost:9092"})

    data=message.map(lambda x: x[1])
    
    def functordd(rdd):
        try:
            rdd1=rdd.map(lambda x: json.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            df.createOrReplaceTempView("Data")
            df1=spark.sql("select coord.lon,coord.lat,main.temp,main.feels_like,main.temp_min,main.temp_max,main.pressure,main.humidity,wind.speed,clouds.all,weather[0].description from Data")
            df1.write.format('csv').mode('append').save("WeatherData")

        except:
            pass
    
   
    data.foreachRDD(functordd)
    
    ssc.start()
    ssc.awaitTermination()