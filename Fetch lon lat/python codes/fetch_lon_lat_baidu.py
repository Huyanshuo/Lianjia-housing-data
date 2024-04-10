import requests
import pandas as pd
import json
import os
import threading
import concurrent.futures
import urllib.parse
import hashlib

global ak
global sk
global url
global host

ak = "IHoEEUVuV7ypkGEHok7hgIlq1HPH1s5A"
sk = "rokDL6w5ZN6PKdEkj8PItOBdRRnZf5mC"
url = "/geocoding/v3"
host = "https://api.map.baidu.com"
 
 

def parse_csv_addr():
    estate_data = []
    totalListData = pd.read_csv("/Users/huyh/Desktop/Mac Lianjia housing data /Fetch lon lat/estate_info.csv")
    totalListDict = totalListData.to_dict('records')
    for estate in totalListDict:
        estate_data.append({
            'id_estate': str(estate['id_estate']),
            'full_location':str(estate['full_location'])
            })
    return estate_data
    
 
def geocode(address):
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

    print(result)
    if result['status'] == '0':
        if result['result']['level'] not in ["省","城市","区县","乡镇","村庄"]:
            res = result['result']['location']
            lon = float(res['lng'])
            lat = float(res['lat'])
            return lon,lat
        else:
            return 0, 0
    else:
        return 0, 0


def process_estate(estate, df):
    lon, lat = geocode(estate['full_location'])
    df.loc[estate['id_estate'], 'long_lat'] = lon, lat
    df.loc[estate['id_estate'], 'lon'] = lon
    df.loc[estate['id_estate'], 'lat'] = lat
    df.loc[estate['id_estate'], 'id_estate'] = estate['id_estate']
    df.loc[estate['id_estate'], 'estate_name'] = estate['full_location']
    print('id = ' + estate['id_estate'] + ':' + '\t' + estate['full_location'] + '，corr = ' + str(lon) + ',' + str(lat))
    
 
    
df = pd.DataFrame(columns=['id_estate','estate_name','long_lat','lon','lat'])
estate_info = parse_csv_addr()
estate_id = 0


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        for estate in estate_info[0:10]:
            executor.submit(process_estate, estate, df)

    df.to_csv('locdetail_ttl.csv', index=False, encoding='utf-8')



#if __name__ == '__main__':
    #threads = []
    #for estate in estate_info:
        #thread = threading.Thread(target=process_estate, args=(estate, df))
        #thread.start()
        #threads.append(thread)
    #for thread in threads:
        #thread.join()
                
    #df.to_csv('locdetail_ttl.csv', index=False, encoding='utf-8')


    #for estate_id in range(300):
        
            #folder = 'output'
                
            #if estate_id % 100 == 0:
                #if not os.path.exists(folder):
                    #os.makedirs(folder)
                #filename = os.path.join(folder, 'locdetail{}.csv'.format( (estate_id // 100) % 5 + 1))
            #if estate_id % 50 == 0:
                #df.to_csv(filename, index=False, encoding='utf-8')
                #print('Saved {} entries to {}'.format(estate_id, filename))

            #estate  = estate_info[estate_id]
            #lon, lat = geocode(estate['full_location'])
            #df.loc[estate_id, 'long_lat'] = lon, lat
            #df.loc[estate_id, 'lon'] = lon
            #df.loc[estate_id, 'lat'] = lat
            #df.loc[estate_id, 'id_estate'] = estate['id_estate']
            #df.loc[estate_id, 'estate_name'] = estate['full_location']
            #print('id = ' + estate['id_estate'] + ':' + '\t' + estate['full_location'] + '，corr = ' + str(lon) + ',' + str(lat) )
            #estate_id += 1

