import requests
import pandas as pd
import json
 
 
# 读取csv文件中 organization 的列，并添加在列表中，示例['地址1','地址2',……,'地址n']
#def parse_csv_addr():
    #estate_data = []
    #totalListData = pd.read_csv('/Users/huyh/Desktop/Fetch lon lat/estate_info.csv')
    #totalListDict = totalListData.to_dict('index')
    #for i in range(0,len(totalListDict)):
        #estate_data.append(str(totalListDict[0][i]['id_estate']))
        #estate_data.append(str(totalListDict[1][i]['full_location']))
    #return estate_data

def parse_csv_addr():
    estate_data = []
    totalListData = pd.read_csv('/Users/huyh/Lianjia-housing-data/Fetch lon lat/estate_info.csv')
    totalListDict = totalListData.to_dict('index')
    for i in range(len(totalListDict)):
        estate_data.append(str(totalListDict[i]['id_estate']))
        estate_data.append(str(totalListDict[i]['full_location']))
    return estate_data
 
# 地址转经纬度 选用的是地理编码的接口
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
        if result['geocodes'][0]['level'] not in ["省","市"]:
            res = result['geocodes'][0]['location']
            print(res)
            return res
        else:
            print("error")
            return "error"
    else:
        print("error")
        return "error"
 
 
if __name__ == '__main__':
    i = 0
    count = 0
    df = pd.DataFrame(columns=['id_estate','estate_name','long_lat'])
    locations = parse_csv_addr()
    while count < 5:
        for location in locations:
            long_lat = geocode(location['full_location'])
            df.loc[i, 'estate_name'] = location['full_location']
            df.loc[i, 'long_lat'] = [long_lat]
            df.loc[i, 'id_estate'] = location['id_estate']
            print(location)
            i = i + 1
            count = count + 1
    df.to_csv('locdetail1.csv', index=False, encoding='utf-8')

