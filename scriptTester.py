# import asyncio
# from bleak import BleakClient

# async def main():
# 	flag = False
# 	async with BleakClient("E1:BE:94:7B:0D:70") as client:
# 		print(client.is_connected)

# 	print(client.is_connected)
# asyncio.run(main())


# Reconnect sensor logic, but ask for connection every time i want data
# import asyncio
# from bleak import BleakClient
# import time

# max_retries = 5  # Change this to desired number of retries
# attempt = 0


# async def reconnect_and_print_status():
#     for attempt in range(max_retries + 1):
#         try:
#             async with BleakClient("F1:05:96:09:C9:5C") as client:
#                 print(f"Connection attempt {attempt}: {client.is_connected}")
#                 # Perform actions with the connected client here
#                 await asyncio.sleep(1)  # You can adjust the delay between attempts
#                 break  # Exit the loop on successful connection
#         except Exception as e:
#             print(f"Error connecting to nsensor (attempt {attempt+1}): {e}")
#             await asyncio.sleep(2)  # Wait before retrying

#     if attempt == max_retries:
#         print(f"Failed to connect to nsensor after {max_retries} retries.")


# async def main():
#     while True:
#         await reconnect_and_print_status()
#         # Add logic for program termination (e.g., using keyboard interrupt)


# asyncio.run(main())


# code to reconnect automatically and keeps connections while reading

import asyncio
from bleak import BleakClient
import time

max_retries = 5  # Change this to desired number of retries
attempt = 0
connected = False


async def read_sensor_data(client):
    """Reads data from the connected BLE sensor.

    This function handles potential exceptions during data reading.

    Replace the placeholder comment with the actual code to read data from
    your specific sensor's characteristics.
    """
    print("Reading sensor data...")
    try:
        # Replace this with the actual code to read data from the sensor
        # (e.g., characteristic reads)
        await asyncio.sleep(1)  # Simulate data reading (replace with actual code)
        print("Data read successfully!")
    except Exception as e:
        print(f"Error reading sensor data: {e}")


async def connect_and_read_data():
    global connected
    for attempt in range(max_retries + 1):
        try:
            async with BleakClient("F1:05:96:09:C9:5C") as client:
                print(f"Connection attempt {attempt}: {client.is_connected}")
                connected = True
                while client.is_connected:  # Loop until connection is lost
                    await read_sensor_data(client)
                    await asyncio.sleep(3)  # Adjustable delay
        except Exception as e:
            print(f"Error connecting to nsensor (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)  # Wait before retrying

    if attempt == max_retries:
        print(f"Failed to connect to nsensor after {max_retries} retries.")
        connected = False


async def main():
    reading_frequency = 5  # Adjust this to desired delay between readings
    while True:
        if not connected:
            await connect_and_read_data()
        # Add logic for program termination (e.g., using keyboard interrupt)


asyncio.run(main())
