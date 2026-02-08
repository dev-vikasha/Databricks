# Databricks notebook source
# MAGIC %pip install confluent_kafka

# COMMAND ----------

# MAGIC %md
# MAGIC #  Implementing Producer Using Confluent Kafka

# COMMAND ----------

from confluent_kafka import Producer
import uuid
import json
producer_config  = {
    "bootstrap.servers" : "localhost:9092"
}

producer = Producer(producer_config)

order = {
    "Order_ID" : str(uuid.uuid4()),
    "Product_ID" : "P21",
    "Product" : "Banana",
    "Quantity" : 12
}

value = json.dumps(order).encode("utf-8")

def delivery_report(err , msg):
    if err is not None:
        print(f"Delivery is Failed❌ due to {err}")
    else: 
        print(f"Your Delivery {msg.value().decode("utf-8")} is Successful")
        print(dir(msg))
        #dir(): returns a list of all attributes & methods available for an object
        print(f"\nDeliverd to topic {msg.topic()} in Partition {msg.partition()}  at Offset {msg.offset()}")
    
producer.produce(topic="orders", value=value, callback = delivery_report)

producer.flush()

# COMMAND ----------

from confluent_kafka import Consumer
import pandas as pd

consumer_config = {
    "bootstrap.servers" : "localhost:9092",
    'group.id' : "order-trackers" ,
    # group id is a unique string that identifies the consumer group consumer belongs to
    "auto.offset.reset" : "earliest"
    # when offset miss & kafka is not able to determine what last read msg is then what to do
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"]) # passing list of topics whatever needed

print("🟢 Consumer is Listning to Subscribe Topics :")

data = [] 
try :
    while True:
        msg = consumer.poll(1.0) # Asks the broker for any new messageson the subscribed topics & returns them to the cosnumer for processing

        if msg is None: continue
        
        if msg.error():
            print("❌ Error : ", msg.error())

        value = msg.value().decode("utf-8") # decoding msg from Bytes to JSON String
        order = json.loads(value) # Converting JSON String to Python Dict Object

        print(f"Recieved Order 📦 {order}")
        data.append(order)

except KeyboardInterrupt : print("🔴 Program is Stopped By User")

finally : consumer.close()

df = pd.DataFrame(data)
print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Kafka Works on Pull Architechture so polling allows to consumers to control how & when & how many times they read the Messages/Events.
# MAGIC
# MAGIC @ Benifits of Pull Architechture :
# MAGIC     - Load Balancing
# MAGIC     - Pausing 
# MAGIC     - Catching Up
# MAGIC     - Simple
# MAGIC     - Reliable
# MAGIC     - Scalable 
# MAGIC     - Controllable - Duration & Frequency when & which they Consume Events
# MAGIC     
