#!/usr/bin/env python
import threading, logging, time
import multiprocessing
from kafka import KafkaConsumer
import json

class Consumer(multiprocessing.Process):
    def __init__(self):
        # key as the stock symbol, value as a tuple two, left is the average, right is the total count
        self.data = {}
        self.stop_event = threading.Event()
        multiprocessing.Process.__init__(self)
        self.interval = 30
        thread = threading.Thread(target=self.log_stock, args=())
        thread.daemon = True                          
        thread.start()
        

    def stop(self):
        self.stop_event.set()

    def log_stock(self):
        while not self.stop_event.is_set():
            print(self.data)
            time.sleep(self.interval)

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                 auto_offset_reset='latest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['stock'])

        while not self.stop_event.is_set():
            for message in consumer:
                res = json.loads(message.value.decode())
                if res['name'] in self.data:
                   key = res['name']
                   value = self.data[key]
                   self.data[key] = ((value[0] * value[1] + res['price']) / (value[1] + 1), value[1] + 1)
                   #print(self.data)
                else:
                   self.data[res['name']] = (res['price'], 1)    
 
                if self.stop_event.is_set():
                    break

        consumer.close()

def main():
    time.sleep(10)
    task = Consumer()
    task.run()

if __name__ == "__main__":
   main()
