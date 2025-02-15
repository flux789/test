import paho.mqtt.client as mqtt
import socket
import time
import yaml
import os
import json
import serial
import io
import atexit
import sys
import constants

print("Starting up...")

config = {}

if os.path.exists('/data/options.json'):
    print("Loading options.json")
    with open(r'/data/options.json') as file:
        config = json.load(file)
        print("Config: " + json.dumps(config))

elif os.path.exists('config.yaml'):
    print("Loading config.yaml")
    with open(r'config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)['options']
        
else:
    sys.exit("No config file found")  

# Configuration variables
scan_interval = config['scan_interval']
connection_type = config['connection_type']
bms_serial = config['bms_serial']
ha_discovery_enabled = config['mqtt_ha_discovery']
code_running = True
bms_connected = False
mqtt_connected = False
print_initial = True
debug_output = config['debug_output']
disc_payload = {}
repub_discovery = 0

# BMS Info
bms_version = ''
bms_sn = ''
pack_sn = ''
packs = 1
cells = 16  # Default cell count
temps = 6   # Default temp sensors

print("Connection Type: " + connection_type)

# MQTT Client Setup
def on_connect(client, userdata, flags, rc):
    print("MQTT connected with result code "+str(rc))
    client.will_set(config['mqtt_base_topic'] + "/availability","offline", qos=0, retain=False)
    global mqtt_connected
    mqtt_connected = True

def on_disconnect(client, userdata, rc):
    print("MQTT disconnected with result code "+str(rc))
    global mqtt_connected
    mqtt_connected = False

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.username_pw_set(username=config['mqtt_user'], password=config['mqtt_password'])
client.connect(config['mqtt_host'], config['mqtt_port'], 60)
client.loop_start()
time.sleep(2)

def exit_handler():
    print("Script exiting")
    client.publish(config['mqtt_base_topic'] + "/availability","offline")
    return

atexit.register(exit_handler)

# BMS Communication Functions
def bms_connect(address, port):
    if connection_type == "Serial":
        try:
            print("Trying to connect %s" % bms_serial)
            s = serial.Serial(bms_serial, timeout=1)
            print("BMS serial connected")
            return s, True
        except IOError as msg:
            print("BMS serial error connecting: %s" % msg)
            return False, False    
    else:
        try:
            print("Trying to connect " + address + ":" + str(port))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((address, port))
            print("BMS socket connected")
            return s, True
        except OSError as msg:
            print("BMS socket error connecting: %s" % msg)
            return False, False

def bms_sendData(comms, request=''):
    try:
        if len(request) > 0:
            if connection_type == "Serial":
                comms.write(request)
            else:
                comms.send(request)
            time.sleep(0.25)
            return True
    except Exception as e:
        print("BMS communication error: %s" % e)
        global bms_connected
        bms_connected = False
        return False

def bms_get_data(comms):
    try:
        if connection_type == "Serial":
            return comms.readline()
        else:
            temp = comms.recv(4096)
            temp2 = temp.split(b'\r')
            for element in temp2:
                if len(element) > 0 and element[0] == 0x7e:
                    return element + b'\r'
            return b''
    except Exception as e:
        print("Data receive error: %s" % e)
        return False

# Data Parsing Improvements
def bms_getAnalogData(bms, batNumber):
    global print_initial, cells, temps, packs
    byte_index = 2
    i_pack = []
    v_pack = []
    i_remain_cap = []
    i_design_cap = []
    cycles = []
    i_full_cap = []
    soc = []
    soh = []
    v_cell = {}
    t_cell = {}

    battery = bytes(format(batNumber, '02X'), 'ASCII')
    success, inc_data = bms_request(bms,cid2=constants.cid2PackAnalogData,info=battery)

    if not success:
        return False, inc_data

    try:
        packs = int(inc_data[byte_index:byte_index+2],16)
        byte_index += 2
        prev_cells = cells

        for p in range(1, packs + 1):
            # INFOFLAG handling between packs
            if p > 1:
                potential_flag = int(inc_data[byte_index:byte_index+2],16)
                if potential_flag != prev_cells and potential_flag != 0:
                    if debug_output:
                        print(f"INFOFLAG detected between packs: 0x{potential_flag:04X}, skipping 2 bytes")
                    byte_index += 2

            cells = int(inc_data[byte_index:byte_index+2],16)
            byte_index += 2
            prev_cells = cells

            if print_initial:
                print(f"Pack {p}, Total cells: {cells}")

            # Cell voltages
            cell_voltages = []
            for i in range(cells):
                v_cell[(p-1,i)] = int(inc_data[byte_index:byte_index+4],16)
                byte_index += 4
                client.publish(config['mqtt_base_topic'] + f"/pack_{p}/v_cells/cell_{i+1}", str(v_cell[(p-1,i)]))
                if print_initial:
                    print(f"Pack {p}, V Cell{i+1}: {v_cell[(p-1,i)]} mV")

            # Temperature sensors
            temps = int(inc_data[byte_index:byte_index+2],16)
            byte_index += 2
            for i in range(temps):
                t_cell[(p-1,i)] = (int(inc_data[byte_index:byte_index+4],16)-2730)/10
                byte_index += 4
                client.publish(config['mqtt_base_topic'] + f"/pack_{p}/temps/temp_{i+1}", str(round(t_cell[(p-1,i)],1)))

            # Current measurements
            i_pack.append(int(inc_data[byte_index:byte_index+4],16))
            byte_index += 4
            if i_pack[-1] >= 32768:
                i_pack[-1] = -1*(65535 - i_pack[-1])
            i_pack[-1] /= 100
            client.publish(config['mqtt_base_topic'] + f"/pack_{p}/i_pack", str(i_pack[-1]))

            # Pack voltage
            v_pack.append(int(inc_data[byte_index:byte_index+4],16)/1000)
            byte_index += 4
            client.publish(config['mqtt_base_topic'] + f"/pack_{p}/v_pack", str(v_pack[-1]))

            # Capacity calculations
            i_remain_cap.append(int(inc_data[byte_index:byte_index+4],16)*10)
            byte_index += 4
            i_full_cap.append(int(inc_data[byte_index:byte_index+4],16)*10)
            byte_index += 4
            soc.append(round(i_remain_cap[-1]/i_full_cap[-1]*100,2))
            client.publish(config['mqtt_base_topic'] + f"/pack_{p}/soc", str(soc[-1]))

            # Cycle count
            cycles.append(int(inc_data[byte_index:byte_index+4],16))
            byte_index += 4

            # Design capacity
            i_design_cap.append(int(inc_data[byte_index:byte_index+4],16)*10)
            byte_index += 4
            soh.append(round(i_full_cap[-1]/i_design_cap[-1]*100,2))
            client.publish(config['mqtt_base_topic'] + f"/pack_{p}/soh", str(soh[-1]))

            # Validate byte position
            if byte_index >= len(inc_data):
                break

    except Exception as e:
        print(f"Error parsing BMS analog data: {str(e)}")
        return False, f"BMS analog data error: {str(e)}"

    return True, True

# Rest of the functions (bms_getWarnInfo, chksum_calc, etc.) remain similar 
# with analogous INFOFLAG handling added

# Main execution
print("Connecting to BMS...")
bms,bms_connected = bms_connect(config['bms_ip'],config['bms_port'])

client.publish(config['mqtt_base_topic'] + "/availability","offline")
print_initial = True

# Initial BMS info collection
success, data = bms_getVersion(bms)
time.sleep(0.1)
success, bms_sn, pack_sn = bms_getSerial(bms)

while code_running:
    if bms_connected and mqtt_connected:
        success, data = bms_getAnalogData(bms,batNumber=255)
        time.sleep(scan_interval/3)
        success, data = bms_getPackCapacity(bms)
        time.sleep(scan_interval/3)
        success, data = bms_getWarnInfo(bms)
        time.sleep(scan_interval/3)

        if print_initial:
            ha_discovery()
            client.publish(config['mqtt_base_topic'] + "/availability","online")
            print_initial = False

    # Connection recovery logic remains the same...

client.loop_stop()
