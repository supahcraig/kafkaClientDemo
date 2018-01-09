from kafka import KafkaProducer
import datetime
from time import sleep
import json
import random
import math

topic = 'MYGtest'

kafka_servers = ['10.4.3.61', '10.4.3.62', '10.4.3.63']
zookeeper_servers = ['10.4.3.24', '10.4.3.25', '10.4.3.26']

# send to kafka
producer = KafkaProducer(bootstrap_servers=kafka_servers,
                         value_serializer = lambda m: json.dumps(m, default=str).encode()
                        )

#demoType = 'rewards'
demoType = 'API'

messageNbr = 0
while True:
    messageNbr += 1

    sleepDelay = 0

    if demoType == 'rewards':
        msg = {'topic': topic,
               'rewardID': messageNbr,
               'rewardDefID': math.floor(random.random() * 10),
               'memberID': math.floor(random.random() * 10000000),
               'rewardIssueDate': datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S")
               }
        sleepDelay = 0.050 + (random.random() * 5)


    elif demoType == 'API':
        API = math.floor(random.random() * 5)

        if API == 0:
            callName = 'GetMember'
        elif API == 1:
            callName = 'AddMember'
        elif API == 2:
            callName = 'AddMemberRewards'
        elif API == 3:
            callName = 'GetAccountSummary'
        elif API == 4:
            callName = 'UpdateMember'

        msg = {'topic': topic,
               'APIcallID': messageNbr,
               'operationName': callName,
               'status': math.floor(random.random() * 2),
               'callTimestamp': datetime.datetime.now().strftime("%Y-%b-%d %H:%M:%S.%f")
               }
        #sleepDelay = random.random()*1


    future = producer.send(topic, msg)

    print(msg)
    sleep(sleepDelay)

    if messageNbr > 100000:
        exit(0)
