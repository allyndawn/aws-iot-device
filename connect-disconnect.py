# AWS IoT MQTT Connect Disconnect Example
#
# Allen Snook
# January 17, 2021

import argparse
from awscrt import io
from awsiot import mqtt_connection_builder
from uuid import uuid4

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

# Disconnect

print("Disconnecting...")
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")