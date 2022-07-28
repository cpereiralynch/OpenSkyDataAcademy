
import asyncio
import requests
import time
#import json
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import os

async def get_stream(producer):
    username =os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    begin = 1517227200
    end = 1517230800
    #upperkeys = ['time', 'states']
    #statesHeaders = ['time','icao24','callsign','origin_country','time_position','last_contact', \
    #             'longitude','latitude','baro_altitude','on_ground','velocity','true_track','vertical_rate' \
    #,'sensors','geo_altitude','squawk','spi','position_source','category']
    while(1):
        response = requests.get("https://opensky-network.org/api/flights/all?begin="+str(begin)+"&end="+str(end),auth  = (username, password))
        print(response.status_code)
        if response.status_code==200:
            await producer.send_event(EventData(response.text))
            print(EventData(response.text))
            #jsonResponse = json.loads(response.text)
            begin += 3600
            end += 3600
        time.sleep(3600)


async def main():
    producer = EventHubProducerClient.from_connection_string(conn_str=os.getenv('CONNECTION_STRING'), eventhub_name=os.getenv('TOPIC_NAME'))
    task = asyncio.create_task(get_stream(producer))
    await task

if __name__ == "__main__":
    asyncio.run(main())

