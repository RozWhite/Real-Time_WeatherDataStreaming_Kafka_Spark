import json
import requests
from kafka import KafkaProducer
from time import sleep

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],
                      value_serializer=lambda x: json.dumps(x).encode('utf-8')
                      )
                    

# Enter your API key here
api_key ="0a66e1a920f80830fa52d4e53937cb69"
 
# base_url variable to store url
base_url = "http://api.openweathermap.org/data/2.5/weather?"
 
# Give city name
city_name = "Toronto"
 
# complete_url variable to store
# complete url address
complete_url = base_url + "appid=" + api_key + "&q=" + city_name

for i in range(50):
    res=requests.get(complete_url)
    data=json.loads(res.content.decode('utf-8'))
    print(data)
    producer.send("WeatherTopic",value=data)
    sleep(5)
    producer.flush()

