from confluent_kafka import Producer
from config import config


def callback(err,event):
    if err:
         print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
       val=event.value().decode('utf-8') if event.key() else ''
       key = event.key().decode('utf-8') if event.key else ''
       ts = event.timestamp()[1]
       print(f'Key: {key}, Value: {val}, Partition: {event.partition()}, TS: {ts}')


def checkProducer(producer,key,value):
    key = str(key).encode('utf-8')
    value = str(value).encode('utf-8')
    
    producer.produce('hello_topic',value,key=key,on_delivery=callback)


if __name__=='__main__':
    producer=Producer(config)

    regions={
        1: "Marmara",
        2: "Ege",
        3: "Akdeniz",
        4: "İç Anadolu",
        5: "Karadeniz",
        6: "Doğu Anadolu",
        7: "Güneydoğu Anadolu"
    }

    for key,value in regions.items():
        checkProducer(producer,key,value)
        producer.flush()


    








   
    






