import requests
import json

"""
id = 'E438'
call = 'stationOverview?stationIds='
url1 = 'https://s3.eu-central-1.amazonaws.com/app-prod-static.warnwetter.de/v16/'
url2 = 'https://app-prod-ws.warnwetter.de/v30/'
url = url2 + call + id
print(url)
"""

api_key = 'b2d5c5b846d9403595c5b846d99035ee'
state = 'germany'
city = 'twistringen'
url = f'http://api.wunderground.com/api/{api_key}/conditions/q/{state}/{city}.json'

req = requests.get(url=url)
json_obj = json.loads(req.text)
print(json_obj)

print("100")
print("0")
print("14")
print("14")
