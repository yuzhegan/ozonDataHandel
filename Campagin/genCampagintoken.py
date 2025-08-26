# encoding='utf-8

# @Time: 2025-08-23
# @File: %
#!/usr/bin/env
import os
import requests
class OzonToken:
    """
    Ozon Token class to handle token retrieval and management.
    """
    def __init__ (self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret

    def genCapaginToken(self):
        url = "https://api-performance.ozon.ru/api/client/token"

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            print("Raw response:", data)

            # 常见字段名处理（若返回为 OAuth 标准结构）
            access_token = data.get("access_token")
            token_type = data.get("token_type", "Bearer")
            if access_token:
                print("Authorization:", f"{token_type} {access_token}")
            else:
                print("未在响应中找到 access_token 字段，请检查返回结构。")

        except requests.HTTPError as e:
            print("HTTPError:", e, "| Response:", getattr(e.response, "text", ""))
        except requests.RequestException as e:
            print("RequestException:", e)
    def campaignList(self):
        """
        Placeholder for campaign list retrieval method.
        """
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en,en-US;q=0.9",
            "api-key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhZHZlcnRpc2luZy5wZXJmb3JtYW5jZS5vem9uLnJ1IiwiZXhwI joxNzU1OTE5OTM4LCJpYXQiOjE3NTU5MTgxMzgsImlzcyI6InBlcmZvcm1hbmNlLWF1dGgub3pvbi5ydSIsInN1YiI6IjU3ODMxMjAyLTE3NDE1NzMyMTU5MDdAYWR2ZXJ 0aXNpbmcucGVyZm9ybWFuY2Uub3pvbi5ydSJ9.uTueceNSCRB2TnKCQHDYiyIdRCqVb6-QwhJNcAFsJYo",
            "cache-control": "no-cache",
            "client-id": "57831202-1741573215907@advertising.performance.ozon.ru",
            "content-type": "application/json",
            "origin": "https://docs.ozon.ru",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://docs.ozon.ru/",
            "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "x-o3-app-name": "sandbox-doc-api"
        }
        url = "https://api-performance.ozon.ru/api/client/campaign"
        response = requests.get(url, headers=headers)

        print(response.text)
        print(response)

if __name__ == "__main__":
    client_id = '57831202-1741573215907@advertising.performance.ozon.ru'
    client_secret = 'eUzxF6qMQm2JxMoYeCTTzOf5Q7T0QkDT7LHxsP1kYpx2DVPimDF-fTzy2Vhl64qCaSt9VT55C_trnqJj2w'

    ozon_token = OzonToken(client_id, client_secret)
    ozon_token.genCapaginToken()
