import requests
import os

WEBHOOK_ID = os.getenv('WEBHOOK_ID')
WEBHOOK_TOKEN = os.getenv('WEBHOOK_TOKEN')
WEBHOOK_URL = "https://discordapp.com/api/webhooks/1337288429733675008/L5yzKpqhuY0f-rIBPQYYNKcDjeaVsYrEUr-n8UTukV_yL_M7w_PmLfZ4_BguAn1D9I-h"
data = { "content": "mkdir이 정상적으로 실행되지 않았습니다" }
response = requests.post(WEBHOOK_URL, json=data)

if response.status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
else:
        print(f"에러 발생: {response.status_code}, {response.text}")