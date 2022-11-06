import requests
import json

id = 'E438'
call = 'stationOverview?stationIds='
url1 = 'https://s3.eu-central-1.amazonaws.com/app-prod-static.warnwetter.de/v16/'
url2 = 'https://app-prod-ws.warnwetter.de/v30/'
url = url2 + call + id
print(url)

json_obj = json.loads(requests.get(url=url).text)
print(json_obj)

print("100")
print("0")
print("14")
print("14")