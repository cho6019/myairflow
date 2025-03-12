import requests
import os

def send_noti(msg):
    WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
    WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    data = { "content": msg }
    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"에러 발생: {response.status_code}, {response.text}")

    return response.status_code
