#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SpaceLocation").getOrCreate()


# In[5]:


weather=spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load("./WeatherData/*.csv").toDF(
 "longitude","latitude", "temperatur(K)", "feels_like(K)","temperatur_min(K)","temperatur_max(K)","pressure(hPa)","humidity(%)","windspeed( meter/sec)","clouds(%)","description"
)


# In[6]:


weather.printSchema()


# In[12]:


weather.show(10,  truncate=False)


# In[13]:


weatherDF = weather.toPandas()


# In[14]:


weatherDF.head()


# In[16]:


weatherDF.dtypes


# In[19]:


# select temperature columns 
data1 = weatherDF[['temperatur(K)']].copy()
data1.dropna(inplace=True)
data2 = weatherDF[['feels_like(K)']].copy()
data2.dropna(inplace=True)
data3 = weatherDF[['temperatur_min(K)']].copy()
data3.dropna(inplace=True)
data4 = weatherDF[['temperatur_max(K)']].copy()

# join data
df_new = data1.join(data2,how='outer',sort=True)
df_new= df_new.join(data3,how='outer',sort=True)
df_new= df_new.join(data4,how='outer',sort=True)
df_new.columns = ['temperatur(K)','feels_like(K)','temperatur_min(K)','temperatur_max(K)']
df_new.head()
    

dr = len(df_new)
df_new.describe()


# In[23]:


import matplotlib.pyplot as plt

get_ipython().run_line_magic('matplotlib', 'inline')
df_new.plot(subplots=True,figsize=(10,6))
plt.show()


# In[24]:


import matplotlib.pyplot as plt
import numpy as np
import math
import requests
from PIL import Image
from io import BytesIO

def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  return (xtile, ytile)
  
def num2deg(xtile, ytile, zoom):
  n = 2.0 ** zoom
  lon_deg = xtile / n * 360.0 - 180.0
  lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
  lat_deg = math.degrees(lat_rad)
  return (lat_deg, lon_deg)
  
def getImageCluster(lat_deg, lon_deg, delta_lat,  delta_long):
    smurl = r"http://a.tile.openstreetmap.org/{0}/{1}/{2}.png"

    # find the correct zoom level
    zoom = 1; nt = 1
    while nt<3 and zoom<=13:
        zoom+=1 # increment zoom level
        xmin, ymax =deg2num(lat_deg, lon_deg, zoom)
        xmax, ymin =deg2num(lat_deg + delta_lat, lon_deg + delta_long, zoom)
        nt = (xmax-xmin)*(ymax-ymin)
    print('Number of tiles: ',nt)
    
    # calculate bounding box for all tiles
    lat1,long1 = num2deg(xmin,ymin,zoom)
    lat2,long2 = num2deg(xmax,ymax,zoom)
    nlong = xmax-xmin
    nlat  = ymax-ymin
    w = (nlong)*256
    h = (nlat)*256
    print('Tiles: ',nlat,nlong)
    bb = [w,h,lat1,lat2,long1,long2]
    
    Cluster = Image.new('RGB',((xmax-xmin+1)*256-1,(ymax-ymin+1)*256-1) ) 
    for xtile in range(xmin, xmax+1):
        for ytile in range(ymin, ymax+1):
            try:
                imgurl=smurl.format(zoom, xtile, ytile)
                imgstr = requests.get(imgurl).content
                tile = Image.open(BytesIO(imgstr))
                Cluster.paste(tile, box=((xtile-xmin)*256 ,  (ytile-ymin)*255))
            except: 
                print("Couldn't download image")
                tile = None
    return [bb,Cluster]    


# In[25]:


import plotly.express as px

df = px.data.carshare()
fig = px.scatter_mapbox(weatherDF, lat="latitude", lon="longitude",                          size="temperatur(K)", text="description",                        
                        color_continuous_scale=px.colors.cyclical.IceFire, size_max=10, zoom=10)
fig.update_layout(
    mapbox_style="open-street-map",
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
)
fig.show()
# 


# In[ ]:




