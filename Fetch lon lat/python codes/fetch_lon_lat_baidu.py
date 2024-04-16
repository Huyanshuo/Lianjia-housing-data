import requests
import pandas as pd
import json
import os
import threading
import concurrent.futures
import urllib.parse
import hashlib
from queue import Queue

global ak
global sk
global url
global host
global folder

ak1 = "N5VzvHm3t1oKGHjfAtsqwpMZJ8LttLxz"
sk1 = "w4cDJRqRNvx15znAxpcvvCEFPC7T028I"

ak2 = "IHoEEUVuV7ypkGEHok7hgIlq1HPH1s5A"
sk2 = "rokDL6w5ZN6PKdEkj8PItOBdRRnZf5mC"

ak = ak1
sk = sk1
url = "/geocoding/v3"
host = "https://api.map.baidu.com"
folder = "/Users/huyh/Desktop/Mac Lianjia housing data /Fetch lon lat/Output"

start_id = 52001
end_id = 57000
 

def parse_csv_addr():
    estate_data = []
    totalListData = pd.read_csv("/Users/huyh/Desktop/Mac Lianjia housing data /Fetch lon lat/estate_info.csv")
    totalListDict = totalListData.to_dict('records')
    for estate in totalListDict:
        estate_data.append({
            'id_estate': int(estate['id_estate']),
            'full_location':str(estate['full_location'])
            })
    return estate_data
    
 
def geocode(address):
    try:
        params = {
            'address': address,
            'output': 'json',
            'ak': ak ,
        }

        paramsArr = []
        for key in params:
            paramsArr.append(key + "=" + params[key])

        queryStr = url + "?" + "&".join(paramsArr)
        encodedStr = urllib.request.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
        rawStr = encodedStr + sk
        sn = hashlib.md5(urllib.parse.quote_plus(rawStr).encode("utf8")).hexdigest()
        queryStr = queryStr + "&sn=" + sn
        full_url = host + queryStr
        response = requests.get(full_url)
        result = response.json()
    
        if result['status'] == 0 and result['result']['level'] not in ["省","城市","区县","乡镇","村庄"]:
            res = result['result']['location']
            lon = float(res['lng'])
            lat = float(res['lat'])
            pre = result['result']['precise']
            confi = result['result']['confidence']
            compre = result['result']['comprehension']
            return lon, lat, pre, confi, compre

        else:
            return 0, 0, 0, 0, 0

    except Exception as e:
        print(f"Error geocoding {address}: {e}")
        return None


def process_estate(estate, df):
    result = geocode(estate['full_location'])
    row_index = estate['id_estate'] - 1
    
    if result is not None:
        lon, lat, pre, confi, compre = result
        lon_lat = (lon, lat)
        result_queue.put((row_index, estate['id_estate'], estate['full_location'], lon_lat, lon, lat, pre, confi, compre))
        print('id = ' + str(estate['id_estate']) + ':' + '\t' + estate['full_location'] + '，corr = ' + str(lon) + ',' + str(lat) + '\n')
    else:
        print(f"Error geocoding {estate['full_location']}")
 
    
df = pd.DataFrame(columns=['id_estate','estate_name','long_lat','lon','lat','precise','confidence','comprehension'])
estate_info = parse_csv_addr()

if __name__ == '__main__':
    result_queue = Queue()
    with concurrent.futures.ThreadPoolExecutor(30) as executor:

        for estate in estate_info[start_id - 1: end_id]:
            executor.submit(process_estate, estate, result_queue)
            estate_id = estate['id_estate']

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    results.sort(key=lambda x: x[0])

    for result in results:
        row_index, id_estate, estate_name, long_lat, lon, lat, precise, confidence, comprehension = result
        df.loc[row_index] = [id_estate, estate_name, long_lat, lon, lat, precise, confidence, comprehension]
                

    if not os.path.exists(folder):
        os.makedirs(folder)
    filename = os.path.join(folder,'longlat_{}_to_{}.csv'.format(start_id, end_id))
    df.to_csv(filename, index=False, encoding='utf-8')
    print('====================completed======================')



