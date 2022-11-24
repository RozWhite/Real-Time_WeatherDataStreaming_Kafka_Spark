# Real-Time End-to-End Streaming with Kafka and Spark
This project is about building a real-time end-to-end weather data streaming pipeline with Kafka and Spark Streaming for the City of Toronto. The weather data from OpenWeatherMap website is ingested from Kafka, the received message from prodcucer is processed by Spark Streaming. Finally, processed data is stored as a csv file in the local system. Apache Kafka is a distributed streaming platform and is used to build real-time streaming data pipelines and real-time streaming applications. OpenWeatherMap is a service that provides weather data, this API allows to retrieve the current weather data for the city of Toronto . Spark Streaming is an extension to the Spark core API that enables data engineers and data scientists to process real-time data.
Pandas and Plotly library are used for the analysis and visualization of weather data.

## Start the Kafka environment in windows
Start Apache Zookeeper. Open a terminal:
```
zookeeper-server-start.bat C:\kafka\config\zookeeper.properties

```
Start the Kafka server. Open another terminal:
```
kafka-server-start.bat C:\kafka\config\server.properties

```
Create a topic to store your events. Open another terminal:
```
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic WeatherTopic

```
Run the console producer . Open another terminal, navigate to the project folder:
```
python weatherkafkaproducer.py

```
Run the console consumer client/ submit your spark job . Open another terminal, navigate to the project folder:
```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0  weatherkafkaspark.py

```
## Spark UI

The web interface of a running Spark application to monitor and inspect Spark job executions in a web browser.
```
http://host.docker.internal:4040/jobs/

```
</br>
<img src="weather.png"  height="400"/>


