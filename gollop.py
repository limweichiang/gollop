# Import standard packages
import argparse
import time # For MQTT timestamp
import pickle # For pickling MQTT to Graphite
import socket # For creating connection to time-series DB
import sys
import struct
import json # For parsing MQTT message
import queue # For threaded send implementation
import threading # For threaded send implementation

# Import AWS IoT SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# Thread Function: Send data to DB receiver
def thread_dbsend():
    while True:
        print("Function " + sys._getframe().f_code.co_name + " waiting on message from queue")
        msg = dbsendq.get()

        msg_len = len(msg)
        msg_sentmark = 0

        while msg_sentmark < msg_len:
            msg_sent = 0

            try:
                msg_sent = dbsock.send(msg[msg_sentmark:])
            except exception as err:
                print("Function " + sys._getframe().f_code.co_name + " error %d: %s" % (err.errno, err.strerror))
                return

            if msg_sent == 0:
                print("Function " + sys._getframe().f_code.co_name + " connection closed")
                return
            else:
                msg_sentmark = msg_sentmark + msg_sent

        #dbsock.sendall(message) # Needs error correction
        dbsendq.task_done()

# Suport Function: Decode MQTT message and pack into Graphite-compatible Pickle message
# json.loads throws JSONDecodeError if message is faulty
def graphite_parse(message):
    topic = message.topic
    payload = json.loads(message.payload.decode('UTF-8'))
    timestamp = time.time()
    metric_list = [];

    for key in payload:
        # Craft Graphite-compatible metric path
        metric_path = "/" + topic + "/" + key
        metric_list.append((metric_path, (timestamp, payload[key])))

    # Have to use "protocol=2"; Graphite-Carbon doesn't like 3
    graphite_payload = pickle.dumps(metric_list, protocol=2)
    graphite_header = struct.pack("!L", len(graphite_payload))
    graphite_message = graphite_header + graphite_payload

    return graphite_message

# Callback Function: Decode MQTT messages and Pickle batch send to Graphite.
# Variables "client" and "userdata" deprecated in AWS MQTT implementation.
def graphite_send(client, userdata, message):
    #print("Message length %s received" % len(message))
    print("Message topic: %s" % message.topic)
    print("Message payload: %s" % message.payload.decode('UTF-8'))

    try:
        graphite_msg = graphite_parse(message)
    except ValueError as err:
        print("Function " + sys._getframe().f_code.co_name + " parsing error: %s" % err)
        return

    # Send it through dbsendq to the socket send thread
    dbsendq.put(graphite_msg)
    return

# Callback Function: Decode and print MQTT message.
# Variables "client" and "userdata" deprecated in AWS MQTT implementation.
def mqtt_print(client, userdata, message):
    #print("Message length %s received" % len(message))
    print("Message topic: %s" % message.topic)
    print("Message payload: %s" % message.payload.decode('UTF-8'))
    return

# Initialize global variables: Socket for database network connection
dbsock = socket.socket()

# Initialize global variables: Queue to database network send thread
dbsendq = queue.Queue()

# Set up CLI options and input
parser = argparse.ArgumentParser(description="AWS IoT MQTT Subsriber")
parser.add_argument("-e", "--endpoint", required=True, help="AWS IoT endpoint")
parser.add_argument("-r", "--ca-cert", required=True, help="AWS IoT root CA certificate path")
parser.add_argument("-i", "--client-id", required=True, help="AWS IoT client ID")
parser.add_argument("-c", "--client-cert", required=True, help="AWS Iot client certificate path")
parser.add_argument("-k", "--client-privkey", required=True, help="AWS Iot client private key path")
parser.add_argument("-t", "--topic", help="MQTT topics to subscribe to, comma delimited string, no spaces")
parser.add_argument("--graphite-host", help="Receiving host for Graphite Pickle messages")
parser.add_argument("--graphite-port", help="Receiving port for Graphite Pickle messages", type=int, choices=range(1, 65535))
args = parser.parse_args()

# Process arguments where needed
aws_topic = args.topic.split(",") if args.topic is not None else [""]
graphiteport = args.graphite_port if args.graphite_port is not None else 2004

# Set up connection to Graphite Carbon server, if server is provided
if args.graphite_host is not None:
    try:
        dbsock = socket.create_connection((args.graphite_host, graphiteport))
    except OSError as err:
        print("Connect to %s:%s failed" % (args.graphite_host, graphiteport))
        print("Error %d: %s" % (err.errno, err.strerror))
        sys.exit(1)

th_dbsend = threading.Thread(target=thread_dbsend)
th_dbsend.start()

# For certificate based connection
myMQTTClient = AWSIoTMQTTClient(args.client_id)
myMQTTClient.configureEndpoint(args.endpoint, 8883)
myMQTTClient.configureCredentials(args.ca_cert, args.client_privkey, args.client_cert)

# AWS IoT SDK sample defaults (Revisit)
myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
#myMQTTClient.disableMetricsCollection()

if not myMQTTClient.connect():
    print("Unable to establish connection. Check your endpoint and credentials")
    sys.exit(-1)

#print("Number of topics %s" % len(aws_topic))

for t in aws_topic:
    print("Subscribing to topic \"%s\"" % t)

    if args.graphite_host is not None:
        if myMQTTClient.subscribe(t, 0, graphite_send):
            print("Success")
        else:
            print("Failed to subscribe to topic \"%s\", exiting..." % t)
            sys.exit(-1)
    else:
        if myMQTTClient.subscribe(t, 0, mqtt_print):
            print("Success")
        else:
            print("Failed to subscribe to topic \"%s\", exiting..." % t)
            sys.exit(-1)

th_dbsend.join()
