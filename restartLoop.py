
import asyncio
from bleak import BleakClient


async def main():
        flag = False
        try:
            async with BleakClient("E1:BE:94:7B:0D:70") as client:
                print(client.is_connected)
        except Exception as e:
            print(e)
            print(client.is_connected)
        finally:
           # asyncio.run(main())
   	    print("finish")	

asyncio.run(main())
