#!/usr/bin/env python3
import subprocess
import json
import time
import paho.mqtt.client as mqtt
import re
import signal
import sys

# MQTT Configuration
MQTT_BROKER = "IP_ADDRESS"
MQTT_PORT = 1883
MQTT_TOPIC = "rtlamr/meter_data"
MQTT_USERNAME = "USERNAME"  
MQTT_PASSWORD = "PASSWORD"  

# Global variables
process = None
client = None

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    if rc == 0:
        print("Successfully connected to MQTT broker")
    else:
        print(f"Failed to connect to MQTT broker, return code: {rc}")

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")

def parse_rtlamr_output(line):

    try:

        r900_start = line.find('R900:{')
        if r900_start != -1:

            data_content = line[r900_start+6:]  
            
            brace_count = 1
            end_pos = len(data_content)
            for i, char in enumerate(data_content):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i
                        break
            
            data_content = data_content[:end_pos+1]
            print(f"Data content: {data_content}")
            
            data = {}
            
            parts = data_content.split()
            i = 0
            while i < len(parts):
                part = parts[i]
                if ':' in part and not part.startswith('Unkn') and not part.startswith('NoUse'):
                    if part.count(':') == 1:
                        key, value = part.split(':', 1)
                        print(f"Parsing {key}:{value}")
                        try:
                            full_value = value
                            while i+1 < len(parts) and not parts[i+1].endswith('}'):
                                if ':' in parts[i+1]:
                                    break  
                                full_value += ' ' + parts[i+1]
                                i += 1
                            # Extract consumption value
                            if 'Consumption' in data:
                                raw_value = str(data['Consumption'])
                                if len(raw_value) >= 7 and not '.' in raw_value:
                                    # Insert decimal after first 3 digits for CCF (hundreds)
                                    data['Consumption'] = float(f"{raw_value[:-6]}.{raw_value[-6:]}")
                                else:
                                    data['Consumption'] = float(raw_value)

                            full_value = full_value.strip()
                            print(f"Full value: {full_value}")

                            if full_value.startswith('0x'):
                                data[key] = int(full_value, 16)
                            else:

                                num_str = re.sub(r'[^0-9]', '', full_value)
                                if num_str:
                                    data[key] = int(num_str)
                        except ValueError as e:
                            print(f"Value error for {key}:{full_value} - {e}")

                            data[key] = full_value
                i += 1

            print(f"Extracted data: {data}")

            mqtt_data = {
                "timestamp": time.time(),
                "raw_line": line,
                "parsed_data": data,
                "consumption": data.get("Consumption", 0),
                "id": data.get("ID", ""),
                "backflow": data.get("BackFlow", 0),
                "leak": data.get("Leak", 0),
                "leak_now": data.get("LeakNow", 0)
            }

            client.publish(MQTT_TOPIC, json.dumps(mqtt_data), qos=1)
            print(f"Published data: Consumption = {mqtt_data['consumption']}, Leak = {mqtt_data['leak']}")
            return True
    except Exception as e:
        print(f"Error parsing line: {line}")
        print(f"Error details: {e}")
        import traceback
        traceback.print_exc()
        return False

def signal_handler(sig, frame):
    print('Stopping rtlamr...')
    if process:
        process.terminate()
    if client:
        client.disconnect()
    sys.exit(0)

def main():
    global process, client
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    
    try:
        print("Connecting to MQTT broker...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        
        print("Starting rtlamr...")
        process = subprocess.Popen(
            ['/home/tee/go/bin/rtlamr', '-centerfreq=915000000', '-msgtype=r900', '-filterid=1852753560'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        print("Processing rtlamr output... (Press Ctrl+C to stop)")
        while True:
            try:
                line = process.stdout.readline()
                if line:
                    line = line.strip()
                    if line:
                        parse_rtlamr_output(line)
                else:
                    if process.poll() is not None:
                        print("rtlamr process ended")
                        break
                    time.sleep(0.1)
            except KeyboardInterrupt:
                print("Keyboard interrupt received")
                break
                
    except Exception as e:
        print(f"Error in main loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if process and process.poll() is None:
            print("Terminating rtlamr process...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if client:
            client.loop_stop()
            client.disconnect()
        print("Cleanup complete")

if __name__ == "__main__":
    main()
