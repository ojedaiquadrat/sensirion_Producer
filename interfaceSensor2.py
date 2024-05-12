""" Ingesting the Co2, Temperature and Humidity parameter from the Sensirion SCD41 Gadget Sensor
    
    Data information related fromt he sensor was obtained from the "Arduino BLE Gadgets
    BLE Communication Protocol.pdf" that contains the details for the BLE protocol

"""

import asyncio
from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic



""" Defining sensor data obtained from the provider"""
second_sensor_mac_address = "E1:BE:94:7B:0D:70"
service_uuid_loggin_interval = "00008001-B38D-4985-720E-0F993A68EE41"
service_uuid_available_samples = "00008002-B38D-4985-720E-0F993A68EE41"
service_uuid_requested_samples = "00008003-B38D-4985-720E-0F993A68EE41"
service_uuid_data_transfer = "00008004-B38D-4985-720E-0F993A68EE41"

""" Object to store the sensor dataframe """
raw_sensor_dataframe = []

""" Scanning sensor data frames period in (second)
    The interval also must be changed in the Sensirion app to modify the Logging interval    
"""
scan_sensor_period = 10.0

""" Callback to read the datafram notifitication given by Bleak library
    
    The first notification contains the header with the information how
    the sampling data are structured. The following frames contains the sampling data.

    Only the latest samplig data is obtainted for this especific case
"""
def notification_handler(characteristic: BleakGATTCharacteristic, data: bytearray):
    global raw_sensor_dataframe
    raw_sensor_dataframe.clear()  #avoid the first the notification and read only the sampling data frame
    raw_sensor_dataframe.append(list(data))


""" Method to obtain the Co2 bytes based on the Complete Data Logger Data Frame """
def getCO2():
    #print(f"\nRaw Sampling Data frame: {raw_sensor_dataframe}") #check the data frame format
    co2_msb = raw_sensor_dataframe[0][7]
    co2_lsb = raw_sensor_dataframe[0][6]
    co2_hex = f"{co2_msb:02x}{co2_lsb:02x}"
    co2_dec = int(co2_hex, 16)
    #print(f"- Co2 concentration: {co2_dec} ppm\n")
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
    temperature = -45 + ((175.0 * co2_dec_ticks ) / ((2 **16) - 1))
    # print(f"- Temperature: {temperature} °C\n")
    return round(temperature,1)
    
""" Method to obtain the Humidity bytes based on the Complete Data Logger Data Frame

    The bytes contains the Humidity ticks and must be tranformed to %.
    the formula to obtain the conversion is obtained from 3.8 Sample Type 7
"""
def getHumidity():
    co2_msb = raw_sensor_dataframe[0][5]
    co2_lsb = raw_sensor_dataframe[0][4]
    co2_hex = f"{co2_msb:02x}{co2_lsb:02x}"
    humidity_dec_ticks = int(co2_hex, 16)
    humidity = ((100.0 * humidity_dec_ticks) / ((2 **16) - 1))
    # print(f"- Humidity: {humidity} %\n")
    return round(humidity, 1)


""" Main fuction to establish connection with the BLE Sensor usign teh Bleak Library """

async def main():

    async with BleakClient(second_sensor_mac_address) as bleSensorClient:
        
        print("\nConnecting...")
        print("Second Sensor Connected!")

        # Request to omit the oldest samples of the sensor that is set by default and get only the latest sample
        # Defined in the Data Logger service  
        await bleSensorClient.write_gatt_char(service_uuid_requested_samples, b'\x01\x00', response=False)
        
        # Check that requested sample was set up.
        logging_interval = await bleSensorClient.read_gatt_char(service_uuid_requested_samples)
        print(f"\nChecking the number of requested samples that sensor is notifying: {logging_interval[0]}")
        print("")

        print("\n******DATA INTAKING SENSIRION SCD41 CO₂ Sensor Demonstrator******\n")
        number_samples = 1
        # Periodically activate notifications to get the environmental parameters.
        while True:

            await bleSensorClient.start_notify(service_uuid_data_transfer, notification_handler)
            await asyncio.sleep(scan_sensor_period)  
            await bleSensorClient.stop_notify(service_uuid_data_transfer)

            co2_concentration = getCO2()
            temperature_centigrades = getTemperature()
            humidity_percentage = getHumidity()

            print(f"------------- Sample #{number_samples} -------------\n")
            print(f"Co2 Concentration is:   {co2_concentration} ppm ")
            print(f"Temperture is:          {temperature_centigrades}  °C")
            print(f"Humidty Percentage is:  {humidity_percentage} %\n\n")
            number_samples += 1

    # Device will disconnect when the block exits.



# Using asyncio.run() is important to ensure that the device disconnects on
# KeyboardInterrupt or other unhandled exception.
asyncio.run(main())





