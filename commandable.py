# AWS IoT MQTT Commandable Publish and Subscribe Example
#
# Allen Snook
# January 17, 2021

import argparse
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
from uuid import uuid4
import time
import random
import json

highspeed = False

parser = argparse.ArgumentParser()
parser.add_argument('--endpoint', required=True, help="Your AWS IoT custom endpoint")
parser.add_argument('--cert', help="Path to client certificate, in PEM format.")
parser.add_argument('--key', help="Path to private key, in PEM format.")
parser.add_argument('--root-ca', help="Path to root certificate authority, in PEM format.")
parser.add_argument('--client-id', default="test-" + str(uuid4()), help="MQTT Client ID")
parser.add_argument('-n', '--thing-name', action='store', required=True, dest='thing_name', help='Thing name')

args = parser.parse_args()

print("Root CA:", args.root_ca)
print("Certificate Path:", args.cert)
print("Private Key Path:", args.key)
print("Thing Name:", args.thing_name)

def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

def on_message_received(topic, payload, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))

def on_command_received(topic, payload, **kwargs):
    global highspeed
    print("Received command from topic '{}': {}".format(topic, payload))
    oldhighspeed = highspeed
    decoded = json.loads(payload)
    if decoded["value"] == "fast":
        highspeed = True
    else:
        highspeed = False
    message = {
        "oldspeed" : "fast" if oldhighspeed else "slow",
        "newspeed": "fast" if highspeed else "slow"
    }
    mqtt_connection.publish(
        topic="device1/d20/speed/ack",
        payload=json.dumps(message),
        qos=mqtt.QoS.AT_LEAST_ONCE)

event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint=args.endpoint,
    cert_filepath=args.cert,
    pri_key_filepath=args.key,
    client_bootstrap=client_bootstrap,
    ca_filepath=args.root_ca,
    on_connection_interrupted=on_connection_interrupted,
    on_connection_resumed=on_connection_resumed,
    client_id=args.client_id,
    clean_session=False,
    keep_alive_secs=6)

# Connect

print("Connecting to {} with client ID '{}'...".format(
    args.endpoint, args.client_id))
connect_future = mqtt_connection.connect()
connect_future.result()
print("Connected!")

# Subscribe to roll ack

print("Subscribing to device1/d20/roll/ack")
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic="device1/d20/roll/ack",
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)

subscribe_result = subscribe_future.result()
print("Subscribed!")

# Subscribe to speed commands

print("Subscribing to device1/d20/speed")
subscribe_future_cmd, packet_id_cmd = mqtt_connection.subscribe(
    topic="device1/d20/speed",
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_command_received)

subscribe_result = subscribe_future_cmd.result()
print("Subscribed!")

# Roll a D20 every 15 seconds until the user presses Ctrl-C

try:
    while True:
        roll = random.randint(1,20)
        print("Rolled a", roll)
        message = {"value" : roll}
        mqtt_connection.publish(
                topic="device1/d20/roll",
                payload=json.dumps(message),
                qos=mqtt.QoS.AT_LEAST_ONCE)
        print("Press Ctrl-C to stop")
        time.sleep(3 if highspeed else 15)
except KeyboardInterrupt:
    pass

# Disconnect

print("Disconnecting...")
# First, give 5 seconds for any lingering acks to arrive
time.sleep(5)
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")