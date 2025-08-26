# encoding='utf-8

# @Time: 2025-08-23
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import requests
import json
headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en,en-US;q=0.9",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhZHZlcnRpc2luZy5wZXJmb3JtYW5jZS5vem9uLnJ1IiwiZXhwIjoxNzU2MDQ2OTM4LCJpYXQiOjE3NTYwNDUxMzgsImlzcyI6InBlcmZvcm1hbmNlLWF1dGgub3pvbi5ydSIsInN1YiI6IjgyMDIxNDgxLTE3NTU5MTg2MjAxMDRAYWR2ZXJ0aXNpbmcucGVyZm9ybWFuY2Uub3pvbi5ydSJ9.-XWzPMv1011traj987f6f-dZM_2xcHW5IRH0DVVZqGk",
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
def campaignList():
    """
    Placeholder for campaign list retrieval method.
    """

    url = "https://api-performance.ozon.ru/api/client/campaign"
    response = requests.get(url, headers=headers).json()



def campaginDailyReport():
    """
    Placeholder for campaign daily report retrieval method.
    """
    url = "https://api-performance.ozon.ru/api/client/statistics/json"
    data = {
        "campaigns": ["16734360"],
        "dateFrom": "2025-08-20",
        "dateTo": "2025-08-21",
        "groupBy": "DATE"
    }
    data = json.dumps(data, separators=(',', ':'))
    response = requests.post(url, headers=headers, data=data).json()
    ic(response)
def checkReportstatus(uuid):
    """
    Placeholder for checking the status of a report.
    """
    url = f"https://api-performance.ozon.ru/api/client/statistics/{uuid}"
    response = requests.get(url, headers=headers).json()
    ic(response)
def generateReport(uuid):
    """
    Placeholder for generating a report.
    """
    url = "https://api-performance.ozon.ru/api/client/statistics/report"
    params = {
        "UUID": uuid
    }
    response = requests.get(url, headers=headers, params=params).json()
    ic(response)




if __name__ == "__main__":
    campaignList()  # Call the function with None as self, since it's not used in this context.
    uuid = campaginDailyReport()  # Call the function with None as self, since it's not used in this context.
    # checkReportstatus('07f0cf33-ccc4-4660-86b8-cd30a80cc1cd')  # Call the function with None as self, since it's not used in this context.
    # generateReport('07f0cf33-ccc4-4660-86b8-cd30a80cc1cd')  # Call the function with None as self, since it's not used in this context.
    # checkReportstatus('25de87ef-2d82-495a-81fe-ce493c8415fc')  # Call the function with None as self, since it's not used in this context.
    # generateReport('25de87ef-2d82-495a-81fe-ce493c8415fc')  # Call the function with None as self, since it's not used in this context.
