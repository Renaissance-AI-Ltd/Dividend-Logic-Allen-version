#基本功能測試
import requests

def lineNotifyMessage(token, msg):

    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }

    payload = {'message': msg }
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code


if __name__ == "__main__":
  token = 'pD9ODB7gtrmYiM4So3UmCoaPigaCE8UpzXUsG8Mdh7k'
  message = '基本功能測試'
  lineNotifyMessage(token, message)