import asyncio
from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
from kafka import *
from kafka.admin import KafkaAdminClient, NewTopic
import json
from time import sleep

"""Connections Attempt Variables"""
max_retries = 5  # Change this to desired number of retries
attempt = 0
connected = False

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
first_broker = "10.2.2.77:9092"
second_broker = "10.2.2.77:9093"
third_broker = "10.2.2.77:9094"
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
    print("Reading sensor data...")
    print("\n******DATA INTAKING SENSIRION SCD41 CO₂ Sensor Demonstrator******\n")

    try:
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
        print(
            f"\nChecking the number of requested samples that sensor is notifying: {logging_interval[0]}"
        )
        print("")
        await bleSensorClient.start_notify(
            service_uuid_data_transfer, notification_handler
        )
        await asyncio.sleep(
            scan_sensor_period
        )  # Simulate data reading (replace with actual code)
        await bleSensorClient.stop_notify(service_uuid_data_transfer)

        # getting data
        co2_concentration = getCO2()
        temperature_centigrades = getTemperature()
        humidity_percentage = getHumidity()

        # formating data to tranfer through kafka
        s1_data = {
            "co2": co2_concentration,
            "tmp": temperature_centigrades,
            "hum": humidity_percentage,
        }

        # send data to kafka brokers
        producer.send(topic_name, value=s1_data)
        number_samples += 1
        print("Data read and sent successfully!")

        # visualizing data on the cmd
        print(f"------------- Sample #{number_samples} -------------\n")
        print(f"Co2 Concentration is:   {co2_concentration} ppm ")
        print(f"Temperture is:          {temperature_centigrades}  °C")
        print(f"Humidty Percentage is:  {humidity_percentage} %\n\n")

    except Exception as e:
        print(f"Error reading sensor data: {e}")


async def connect_and_read_data():
    global connected
    for attempt in range(max_retries + 1):
        try:
            async with BleakClient(second_sensor_mac_address) as bleSensorClient:
                print(f"Connection attempt {attempt}: {bleSensorClient.is_connected}")
                connected = True
                while bleSensorClient.is_connected:  # Loop until connection is lost
                    await read_sensor_data(bleSensorClient)
                    await asyncio.sleep(3)  # Adjustable delay

        except Exception as e:
            print(f"Error connecting to nsensor (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)  # Wait before retrying

    if attempt == max_retries:
        print(f"Failed to connect to nsensor after {max_retries} retries.")
        connected = False


"""Main Function"""


async def main():
    reading_frequency = 5  # Adjust this to desired delay between readings
    while True:
        if not connected:
            await connect_and_read_data()
        # Add logic for program termination (e.g., using keyboard interrupt)


asyncio.run(main())
