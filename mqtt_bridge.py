#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time
import json
import os
import glob
import logging
import logging.handlers
import threading
import configparser
from collections import deque
import uuid

# Default configuration
DEFAULT_CONFIG = {
    'BROKER': 'localhost',
    'PORT': '1883',
    'MQTT_USER': 'mqtt',
    'MQTT_PASS': 'user',
    'CONFIG_DIR': './library',
    'RECONNECT_DELAY': '5',
    'LOG_FILE': './mqtt_bridge.log',
    'LOG_MAX_SIZE': '10485760',
    'LOG_BACKUP_COUNT': '5',
    'LOG_LEVEL': 'INFO'
}

CONFIG_FILE = 'mqtt-bridge.conf'

def load_config():
    config = configparser.ConfigParser()
    config['DEFAULT'] = DEFAULT_CONFIG
    
    if os.path.exists(CONFIG_FILE):
        try:
            config.read(CONFIG_FILE)
            logging.info(f"Loaded configuration from {CONFIG_FILE}")
        except Exception as e:
            logging.error(f"Failed to read config file: {e}")
    else:
        logging.warning(f"Config file {CONFIG_FILE} not found, using defaults")
        try:
            with open(CONFIG_FILE, 'w') as f:
                for key, value in DEFAULT_CONFIG.items():
                    f.write(f"{key} = {value}\n")
            logging.info(f"Created default configuration file: {CONFIG_FILE}")
        except Exception as e:
            logging.error(f"Failed to create config file: {e}")
    
    return config['DEFAULT']

def setup_logging(config):
    log_level = getattr(logging, config['LOG_LEVEL'].upper(), logging.INFO)
    
    file_handler = logging.handlers.RotatingFileHandler(
        filename=config['LOG_FILE'],
        maxBytes=int(config['LOG_MAX_SIZE']),
        backupCount=int(config['LOG_BACKUP_COUNT']),
        encoding='utf-8'
    )
    
    console_handler = logging.StreamHandler()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Global variables
IR_LIBRARY = {}
mqtt_client = None
is_connected = False
startup_time = None
config = None
pending_commands = {}  # Track sent commands: {status_topic: {command_topic, input_val, output}}

def log_message(direction, topic, payload):
    direction_symbol = "→" if direction == "out" else "←"
    truncated_payload = payload[:100] + "..." if len(payload) > 100 else payload
    logging.info(f"{direction_symbol} {topic} = {truncated_payload}")

def load_library():
    global IR_LIBRARY
    
    IR_LIBRARY = {}
    config_dir = config['CONFIG_DIR']
    
    if not os.path.exists(config_dir):
        logging.error(f"Config directory not found: {config_dir}")
        return
    
    file_count = 0
    command_count = 0
    
    for file_path in glob.glob(os.path.join(config_dir, "*")):
        if os.path.isdir(file_path):
            continue
            
        try:
            with open(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    
                    try:
                        name, payload, cmd_topic, input_val, status_msg, status_topic, output_val = line.split(maxsplit=6)
                        command_topic = f"library/ir/{name}"
                        
                        if command_topic not in IR_LIBRARY:
                            IR_LIBRARY[command_topic] = {}
                        
                        IR_LIBRARY[command_topic][input_val] = {
                            "payload": payload,
                            "cmd_topic": cmd_topic,
                            "output": output_val,
                            "status_topic": status_topic,
                            "status_msg": status_msg,
                            "command_name": name
                        }
                        
                        command_count += 1
                        logging.debug(f"Loaded: {command_topic}[{input_val}] → {output_val}")
                    except Exception as e:
                        logging.warning(f"Parse error {file_path}:{line_num}: {e}")
            
            file_count += 1
        except Exception as e:
            logging.error(f"File error {file_path}: {e}")
    
    logging.info(f"Loaded {command_count} commands from {file_count} files in {len(IR_LIBRARY)} topics")

def reconnect():
    global is_connected
    
    reconnect_delay = int(config['RECONNECT_DELAY'])
    
    while True:
        if not is_connected:
            logging.info(f"Attempting to connect to {config['BROKER']}:{config['PORT']}")
            try:
                mqtt_client.reconnect()
                time.sleep(reconnect_delay)
            except Exception as e:
                logging.error(f"Reconnect failed: {e}")
                time.sleep(reconnect_delay)
        else:
            time.sleep(1)

def on_connect(client, userdata, flags, rc):
    global is_connected, startup_time
    
    if rc == 0:
        is_connected = True
        startup_time = time.time()
        logging.info(f"Connected to MQTT broker {config['BROKER']}:{config['PORT']} (code: {rc})")
        
        for topic in IR_LIBRARY.keys():
            client.subscribe(topic)
            logging.debug(f"Subscribed to command: {topic}")
        
        status_topics = set()
        for commands in IR_LIBRARY.values():
            for entry in commands.values():
                status_topics.add(entry["status_topic"])
        
        for topic in status_topics:
            client.subscribe(topic)
            logging.debug(f"Subscribed to status: {topic}")
    else:
        is_connected = False
        logging.error(f"Connection failed (code: {rc})")

def on_disconnect(client, userdata, rc):
    global is_connected
    is_connected = False
    
    if rc != 0:
        logging.warning(f"Unexpected disconnect (code: {rc}). Reconnecting...")
    else:
        logging.info("Disconnected normally")

def on_message(client, userdata, msg):
    global pending_commands
    
    # Ignore messages received during first 3 seconds after startup
    if startup_time and (time.time() - startup_time) < 3:
        logging.debug(f"Ignoring message on startup: {msg.topic}")
        return
    
    topic = msg.topic
    payload = msg.payload.decode()
    
    log_message("in", topic, payload)
    
    # Handle commands from library topics
    if topic in IR_LIBRARY:
        input_val = payload.strip()
        
        if input_val in IR_LIBRARY[topic]:
            entry = IR_LIBRARY[topic][input_val]
            
            # Send IR command
            try:
                payload_json = json.loads(entry["payload"])
                send_payload = json.dumps(payload_json)
            except:
                send_payload = entry["payload"]
            
            client.publish(entry["cmd_topic"], send_payload)
            log_message("out", entry["cmd_topic"], send_payload)
            
            # Track this command as pending
            pending_commands[entry["status_topic"]] = {
                "command_topic": topic,
                "input_val": input_val,
                "output_val": entry["output"],
                "timestamp": time.time()
            }
            logging.debug(f"Tracked pending command: {entry['status_topic']} → {entry['output']}")
    
    # Handle status/responses from IR devices
    elif topic in pending_commands:
        pending_data = pending_commands.pop(topic, None)
        if not pending_data:
            return
        
        payload_str = payload
        
        # Check if response matches expected status message
        entry = IR_LIBRARY[pending_data["command_topic"]][pending_data["input_val"]]
        status_msg = entry["status_msg"]
        
        try:
            expected_json = json.loads(status_msg)
            payload_json = json.loads(payload_str)
            
            if all(payload_json.get(k) == v for k, v in expected_json.items()):
                # Publish the CORRECT output based on the input that was sent
                status_topic = f"{pending_data['command_topic']}/status"
                client.publish(status_topic, pending_data["output_val"], retain=True)
                log_message("out", status_topic, pending_data["output_val"])
        except:
            if status_msg in payload_str:
                status_topic = f"{pending_data['command_topic']}/status"
                client.publish(status_topic, pending_data["output_val"], retain=True)
                log_message("out", status_topic, pending_data["output_val"])
        
        logging.debug(f"Cleared pending command for {topic}")

def initialize_mqtt():
    global mqtt_client
    
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(config['MQTT_USER'], config['MQTT_PASS'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)
    
    reconnect_thread = threading.Thread(target=reconnect, daemon=True)
    reconnect_thread.start()
    
    return mqtt_client

def cleanup_pending_commands():
    """Clean up old pending commands"""
    global pending_commands
    
    current_time = time.time()
    to_remove = []
    
    for status_topic, data in pending_commands.items():
        if current_time - data["timestamp"] > 30:  # 30 second timeout
            to_remove.append(status_topic)
            logging.warning(f"Timeout for pending command: {data['command_topic']} = {data['input_val']}")
    
    for topic in to_remove:
        pending_commands.pop(topic, None)

def main():
    global config
    
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(config)
    
    logging.info("=" * 60)
    logging.info("MQTT-IR Bridge with Command Tracking")
    logging.info(f"Log file: {config['LOG_FILE']} (max {int(config['LOG_MAX_SIZE'])/1048576:.1f} MB)")
    logging.info("=" * 60)
    
    # Load library
    load_library()
    
    if not IR_LIBRARY:
        logging.error("No commands loaded. Exiting.")
        return
    
    # Initialize MQTT
    client = initialize_mqtt()
    
    try:
        client.connect(config['BROKER'], int(config['PORT']), 60)
        client.loop_start()
        
        # Main loop with periodic cleanup
        while True:
            time.sleep(1)
            cleanup_pending_commands()
            
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        logging.info("Bridge stopped")

if __name__ == "__main__":
    main()
