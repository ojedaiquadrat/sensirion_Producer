import asyncio
from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
from kafka import *
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from standalone.simple_manipulator import predict
import numpy

"""Connections Attempt Variables"""
max_retries = 5  # Change this to desired number of retries
attempt = 0
sensor_status = False
connected = 0

""" Object to store the sensor dataframe """
raw_sensor_dataframe = []

""" Scanning sensor data frames period in (second)
    The interval also must be changed in the Sensirion app to modify the Logging interval    
"""
scan_sensor_period = 10.0

""" Defining sensor data obtained from the provider"""
second_sensor_mac_address = "E1:BE:94:7B:0D:70"
service_uuid_loggin_interval = "00008001-B38D-4985-720E-0F993A68EE41"
service_uuid_available_samples = "00008002-B38D-4985-720E-0F993A68EE41"
service_uuid_requested_samples = "00008003-B38D-4985-720E-0F993A68EE41"
service_uuid_data_transfer = "00008004-B38D-4985-720E-0F993A68EE41"


""" Kafka brokers and topic parameters """
# Topic name
topic_name = "sensirion_1"
topic_partitions = 3
topic_replication = 3


# Broker list creation
# first_broker = "192.168.1.128:9092"
# second_broker = "192.168.1.128:9093"
# third_broker = "192.168.1.128:9094"
# first_broker = "10.2.2.77:9092"
# second_broker = "10.2.2.77:9093"
# third_broker = "10.2.2.77:9094"
first_broker = "10.1.6.83:9092"
second_broker = "10.1.6.83:9093"
third_broker = "10.1.6.83:9094"
kafka_broker_list = [first_broker, second_broker, third_broker]

# Producer creation
producer = KafkaProducer(
    bootstrap_servers=kafka_broker_list,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
)


""" Callback to read the datafram notifitication given by Bleak library
    
    The first notification contains the header with the information how
    the sampling data are structured. The following frames contains the sampling data.

    Only the latest samplig data is obtainted for this especific case
"""


def notification_handler(characteristic: BleakGATTCharacteristic, data: bytearray):
    global raw_sensor_dataframe
    raw_sensor_dataframe.clear()  # avoid the first the notification and read only the sampling data frame
    raw_sensor_dataframe.append(list(data))


""" Method to obtain the Co2 bytes based on the Complete Data Logger Data Frame """


def getCO2():
    # print(f"\nRaw Sampling Data frame: {raw_sensor_dataframe}") #check the data frame format
    co2_msb = raw_sensor_dataframe[0][7]
    co2_lsb = raw_sensor_dataframe[0][6]
    co2_hex = f"{co2_msb:02x}{co2_lsb:02x}"
    co2_dec = int(co2_hex, 16)
    # print(f"- Co2 concentration: {co2_dec} ppm\n")
    return co2_dec


""" Method to obtain the Temperature bytes based on the Complete Data Logger Data Frame

    The bytes contains the temperature ticks and must be tranformed to °C.
    the formula to obtain the conversion is obtained from 3.8 Sample Type 7
"""


def getTemperature():
    co2_msb = raw_sensor_dataframe[0][3]
    co2_lsb = raw_sensor_dataframe[0][2]
    co2_hex = f"{co2_msb:02x}{co2_lsb:02x}"
    co2_dec_ticks = int(co2_hex, 16)
    temperature = -45 + ((175.0 * co2_dec_ticks) // ((2**16) - 1))
    # print(f"- Temperature: {temperature} °C\n")
    # return round(temperature,0)
    return int(temperature)


""" Method to obtain the Humidity bytes based on the Complete Data Logger Data Frame

    The bytes contains the Humidity ticks and must be tranformed to %.
    the formula to obtain the conversion is obtained from 3.8 Sample Type 7
"""


def getHumidity():
    co2_msb = raw_sensor_dataframe[0][5]
    co2_lsb = raw_sensor_dataframe[0][4]
    co2_hex = f"{co2_msb:02x}{co2_lsb:02x}"
    humidity_dec_ticks = int(co2_hex, 16)
    humidity = (100.0 * humidity_dec_ticks) // ((2**16) - 1)
    # print(f"- Humidity: {humidity} %\n")
    # return round(humidity, 0)
    return int(humidity)


def getEnvVariables():
    co2 = getCO2()
    temperature = getTemperature()
    humidity = getHumidity()
    return {"co2": co2, "temp": temperature, "hum": humidity}


async def read_sensor_data(bleSensorClient):
    """Reads data from the connected BLE sensor.

    This function handles potential exceptions during data reading.

    Replace the placeholder comment with the actual code to read data from
    your specific sensor's characteristics.
    """
    
    try:
        
        print("\n******DATA INTAKING SENSIRION SCD41  Sensor Demonstrator******\n")
        print("Reading sensor data...")

        # Replace this with the actual code to read data from the sensor
        # (e.g., characteristic reads)

        number_samples = 1
        # Request to omit the oldest samples of the sensor that is set by default and get only the latest sample
        # Defined in the Data Logger service
        await bleSensorClient.write_gatt_char(
            service_uuid_requested_samples, b"\x01\x00", response=False
        )
        # Check that requested sample was set up.
        logging_interval = await bleSensorClient.read_gatt_char(
            service_uuid_requested_samples
        )
        #print(f"\nChecking the number of requested samples that sensor is notifying: {logging_interval[0]}")
        print("")
        await bleSensorClient.start_notify(service_uuid_data_transfer, notification_handler)
        await asyncio.sleep(scan_sensor_period)  # Simulate data reading (replace with actual code)
        await bleSensorClient.stop_notify(service_uuid_data_transfer)

        global connected
        # getting data
        if sensor_status == True:
            co2_concentration = getCO2()
            temperature_centigrades = getTemperature()
            humidity_percentage = getHumidity()
            connected = 1
            co2_validation = predict("co2", co2_concentration)
            co2_validation_send = int(co2_validation[0])
            tmp_validation = predict("temperature", temperature_centigrades)
            tmp_validation_send = int(tmp_validation[0])
            hum_validation = predict ("humidity", humidity_percentage)
            hum_validation_send = int(hum_validation[0])

        # formating data to tranfer through kafka
        s1_data = {
            "co2": co2_concentration,
            "tmp": temperature_centigrades,
            "hum": humidity_percentage,
            "co2Valid": co2_validation_send,
            "tmpValid": tmp_validation_send,
            "humValid": hum_validation_send,
            "stat": connected,
        }

         # visualizing data on the cmd
        print(f"------------- Samples -------------\n")
        print(f"Co2 Concentration is:   {co2_concentration} ppm         Validation status: {co2_validation_send} ")
        print(f"Temperture is:          {temperature_centigrades}  °C   Valitadion status: {tmp_validation_send}")
        print(f"Humidty Percentage is:  {humidity_percentage} %        Validations status: {hum_validation_send}")
        print(f"Sensirion-1 status: {connected}\n")

        # send data to kafka brokers
        producer.send(topic_name, value=s1_data)
        number_samples += 1
       
        # force all buffered messages to be sent to the Kafka broker immediately
        producer.flush()
        print("Data read and sent successfully!\n")

       

    except Exception as e:
        print(f"Error reading sensor data: {e}")


async def connect_and_read_data():
    global sensor_status
    for attempt in range(max_retries + 1):
        try:
            async with BleakClient(second_sensor_mac_address) as bleSensorClient:
                print(f"Connection attempt {attempt}: {bleSensorClient.is_connected}")
                sensor_status = True
                while bleSensorClient.is_connected:  # Loop until connection is lost
                    start_time = time.time()
                    await read_sensor_data(bleSensorClient)
                    await asyncio.sleep(5)  # Adjustable delay
                    end_time = time.time()
                    producer_frequency= end_time -start_time
                    print("\nfrequency Producer: ", producer_frequency )

        except Exception as e:
            ##added to send defaults values when the sensor is disconnected
            start_time = time.time()
            co2_concentration = 0
            temperature_centigrades = 0
            humidity_percentage = 0
            connected = 0
            co2_validation_send = -1
            tmp_validation_send = -1 
            hum_validation_send = -1
            s1_data = {
            "co2": co2_concentration,
            "tmp": temperature_centigrades,
            "hum": humidity_percentage,
            "co2Valid": co2_validation_send,
            "tmpValid": tmp_validation_send,
            "humValid": hum_validation_send,
            "stat": connected,
            }

            producer.send(topic_name, value=s1_data)
            producer.flush()
            print(f"Error connecting to nsensor (attempt {attempt+1}): {e}")
            # visualizing data on the cmd
             # visualizing data on the cmd
            print(f"------------- Samples -------------\n")
            print(f"Co2 Concentration is:   {co2_concentration} ppm        Validation status: {co2_validation_send} ")
            print(f"Temperture is:          {temperature_centigrades}  °C   Valitadion status: {tmp_validation_send}")
            print(f"Humidty Percentage is:  {humidity_percentage} %        Validations status: {hum_validation_send}")
            print(f"Sensirion-1 status: {connected}\n")
            await asyncio.sleep(30)  # Wait before retrying
            end_time = time.time()
            exception_time = end_time -start_time
            print(f"The error frequency is : {exception_time}")

    
        if attempt == max_retries:
            print(f"Failed to connect to Sensor-1 after {max_retries} retries.")
            sensor_status = False


"""Main Function"""
async def main():
    reading_frequency = 5  # Adjust this to desired delay between readings
    while True:
        if not sensor_status:
            await connect_and_read_data()
        # Add logic for program termination (e.g., using keyboard interrupt)
        else:
            producer.close()


asyncio.run(main())


