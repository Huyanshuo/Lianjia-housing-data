import requests
import pandas as pd
import json
import os
import threading
import concurrent.futures
 
 

def parse_csv_addr():
    estate_data = []
    totalListData = pd.read_csv("C:/Users/lenovo/OneDrive - HKUST Connect/Desktop/UROP1100/Fetch lon lat/estate_info.csv")
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
        'key': 'd11cf3b640c8e9c534c33398c61ba4c2',
        'city': '北京市'
    }
    url = 'https://restapi.amap.com/v3/geocode/geo'
    data = requests.get(url, params)
    result = data.json()
    if result['status'] == '1':
        if result['geocodes'][0]['level'] not in ["省","市","区县"]:
            res = result['geocodes'][0]['location']
            lon,lat = res.split(',')
            lon = float(lon)
            lat = float(lat)
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
        for estate in estate_info[1:500]:
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

