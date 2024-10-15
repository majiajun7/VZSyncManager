import json
import time
import psycopg2
import logging
import requests


def dingtalk(content):
    url = "https://oapi.dingtalk.com/robot/send?access_token=429cad2336820f58e4fb401eae506badf1330e25b34b28d5601cd226688ce80e"
    header = {"Content-Type": "application/json"}
    message_data = json.dumps({
        "text": {
            "content": content
        },
        "msgtype": "text"
    })

    # 消息发送失败则尝试重试3次，如未成功输出错误日志并不再尝试。
    n = 1
    while n <= 3:
        n += 1
        response = requests.post(url, message_data, headers=header)

        if response.status_code == 200:
            logging.info('钉钉通知发送成功')
            break
        else:
            logging.error('钉钉通知发送失败，状态码：%d，原因：%s' % (response.status_code, response.text))


if __name__ == "__main__":
    conn = psycopg2.connect(host='10.20.120.238', user='postgres', password='^cA&PVp4rrR3Tvs^HPiQ', dbname='script')
    cur = conn.cursor()
    date = time.strftime('%Y-%m-%d', time.localtime())
    sql = f'SELECT "count"(*) FROM "vpn_application" WHERE "applicationDate" = \'%s\';' % date
    cur.execute(sql)
    result = cur.fetchall()[0][0]
    dingtalk('截止到 %s ，当天VPN网络权限申请单共处理 %s 个' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), result))
