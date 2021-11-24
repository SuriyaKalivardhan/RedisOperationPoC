import os
from redis.client import PubSub
from main import *
import time
import datetime
import threading
import redis

def connect_redis(socket_timeout=30):
    host = "abc.redis.cache.windows.net"
    #host = "127.0.0.1"
    port = 6380
    passwd = "7z...="
    while True:
        r = redis.Redis(host=host, port=port, decode_responses=True, password=passwd, ssl=True, socket_timeout=socket_timeout)
        try:
            r.ping()
        except redis.exceptions.ConnectionError as e:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[INIT] ", e)
            continue
        #print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[INIT] REDIS successfull")
        return r

class TestModule(object):

    python_producer_channel = "goproducerchannel"    

    def _refresh_pubsub(self):
        start = time.time()
        refreshed_pubsub = None
        for i in itertools.count():
            try:
                refreshed_pubsub = self.client.pubsub()
                refreshed_pubsub.subscribe(self.python_producer_channel)
                refreshed_pubsub.psubscribe("suriya*")
                refreshed_pubsub.subscribe("AzureRedisEvents")
            except Exception as e:
                if time.time() - start > 600:
                    print(f"[REDIS REFRESH] Giving up on refreshing pubsub on {e}, crashing")
                    os._exit(3)
                    raise
                duration = min(2 ** i, 5)
                print(f"[REDIS REFRESH] Could not refresh pubsub  {e}, waiting {duration}s")
                time.sleep(duration)
                continue
            else:                
                if refreshed_pubsub is not None:
                    self.pubsub.close()
                    self.pubsub = refreshed_pubsub
                break

    def refresh_subscription(self, channel=python_producer_channel):
        attempts = 10
        delay = 1
        pubSub = self.client.pubsub()
        while True:
            try:
                if channel == self.python_producer_channel:
                    kwargs = self.client.connection_pool.connection_kwargs
                    print(f"{kwargs['host']}:{kwargs['port']}")

                pubSub = self.client.pubsub()
                pubSub.subscribe(channel)
                break
            except Exception as ex:
                attempts = attempts - 1
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[Refresh PubSub] ", ex)
                time.sleep(delay)
                if attempts <= 0:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[Refresh PubSub] ", "Returning most recent attempt")
                    break
        return pubSub

    def producer(self):
        counter=0
        while True:
            try:
                nSub = self.client.publish(self.python_producer_channel, counter)
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[PRODUCER] ", counter, nSub)
            except Exception as ex:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------PRODUCER----------] ", ex)
            time.sleep(1)
            counter = counter + 1

    def openaiconsumer(self):
        while True:
            try:
                result = self.pubsub.get_message(timeout=1)
                if result == None:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[CONSUMER] is NONE, re-establishing connection")
                    self._refresh_pubsub()
                elif result['type'] == 'subscribe' or result['type'] == 'psubscribe':
                    continue
                else:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[CONSUMER] ", result['data'])
            except Exception as ex:                
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------CONSUMER----------] ", ex)  
                self._refresh_pubsub()
    
    def consumer(self):
        pubsub = self.refresh_subscription()
        while True:
            try:
                result = pubsub.get_message(timeout=10)
                if result == None:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[CONSUMER] is NONE, re-establishing connection")
                    pubsub = self.refresh_subscription()
                else:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[CONSUMER] ", result['data'])
            except Exception as ex:                
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------CONSUMER----------] ", ex)                
                pubsub = self.refresh_subscription()
    
    def eventlistener(self):      
        pubsub1 = self.refresh_subscription("AzureRedisEvents")
        while True:
            try:
                result = pubsub1.get_message(timeout=15)
                if result == None:   
                    pubsub1 = self.refresh_subscription("AzureRedisEvents")
                    pass
                elif result['data'] != 1:
                    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[LISTENER] ", result['data'])
            except Exception as ex:                
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------LISTENER----------] ", ex) 
                pubsub1 = self.refresh_subscription("AzureRedisEvents")

    def push(self):     
        while True:
            try:
                result = self.client.rpush("mylist", "myvalue")
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[PUSH] ", result)
                time.sleep(1)
            except Exception as ex:                
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------PUSH----------] ", ex) 

    def remove(self):      
        while True:
            try:
                result = self.client.lrem("mylist", -1, "myvalue")
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[REMOVE] ", result)
                time.sleep(2)
            except Exception as ex:                
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "[-------REMOVE----------] ", ex) 
    
    def __init__(self) -> None:
        self.client = connect_redis()

        self.pubsub = self.client.pubsub()
        self.pubsub.subscribe(self.python_producer_channel)
        self.pubsub.psubscribe("suriya*")
        self.pubsub.subscribe("AzureRedisEvents")

        threading.Thread(target=self.producer, daemon=True).start()
        threading.Thread(target=self.consumer, daemon=True).start()
        threading.Thread(target=self.openaiconsumer, daemon=True).start()
        threading.Thread(target=self.eventlistener, daemon=True).start()
        threading.Thread(target=self.push, daemon=True).start()
        threading.Thread(target=self.remove, daemon=True).start()

    