import urllib.parse
import hashlib

queryStr = '/geocoder/v2/?address=' +'北京市朝阳区10AM新坐标' +'&output=json&ak=yourak'

encodedStr = urllib.parse.quote(queryStr, safe="/:=&?#+!$,;'@()*[]")

rawStr = encodedStr + 'rokDL6w5ZN6PKdEkj8PItOBdRRnZf5mC'

sn = hashlib.md5(urllib.parse.quote_plus(rawStr).encode('utf-8')).hexdigest()

print(sn)
