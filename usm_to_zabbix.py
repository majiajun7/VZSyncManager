import time

import requests
import pyzabbix

import send_message
from log_handler import get_logger


class Usm:
    def __init__(self):
        self.header = {"AccessToken": "40e8f7276c67dbf2b6857ac6c92510bf",
                       "Content-Type": "application/json"}
        self.host = "https://usm.das-security.cn:65080/"

    def get_hosts_by_group(self, group_id):
        url = "%sapi/hostGroups/%s/hosts" % (self.host, group_id)
        response = requests.get(url, headers=self.header, verify=False)
        return response.json()


class ExitException(Exception):
    pass


if __name__ == '__main__':
    logger = get_logger(__name__)
    requests.packages.urllib3.disable_warnings()

    logger.info('开始执行交换机资产数据同步脚本。')
    start_time = time.time()

    try:
        usm = Usm()
        metadata = usm.get_hosts_by_group(44)
        # print(metadata)
        if 'hosts' in metadata:
            logger.info('获取交换机资产成功，开始同步')
            zapi = pyzabbix.ZabbixAPI("http://10.20.120.239")
            zapi.login(api_token='43024175e7f04e082d8a9bfae950505705b35bec7bd799ab1f53c0412e52b0f9')
            for host in metadata['hosts']:
                # print(host['hostName'], host['hostIp'])
                interface = [
                    {
                        "type": 2,
                        "main": 1,
                        "useip": 1,
                        "ip": host['hostIp'],
                        "dns": "",
                        "port": "161",
                        "details": {
                            "version": 2,
                            "bulk": 1,
                            "community": "{$SNMP_COMMUNITY}",
                        }
                    }
                ]
                zabbix_host_name = '*' + host['hostName'] + '-' + host['hostIp'] + '*'
                try:
                    search_host = zapi.host.get(
                        search={"host": zabbix_host_name},
                        searchWildcardsEnabled=True)  # 启用通配符搜索
                except Exception as e:
                    logger.error('交换机 %s 主机查找失败:%s' % (zabbix_host_name, e))
                    raise ExitException()

                if not search_host:
                    try:
                        zapi.host.create(host=zabbix_host_name, groups=[{"groupid": "5"}],
                                         templates={"templateid": 17271}, interfaces=interface)
                    except Exception as e:
                        logger.error('交换机 %s 主机创建失败:%s' % (zabbix_host_name, e))
                    else:
                        logger.info('交换机 %s 主机创建成功' % zabbix_host_name)


        else:
            logger.error('未获取到堡垒机资产信息,请检查')
            raise ExitException()

    except ExitException:
        logger.error('程序遇到错误并退出')

    else:
        end_time = time.time()
        process_time = end_time - start_time
        logger.info('交换机资产数据同步完毕，耗时：%d 秒。' % process_time)

    message_content = '\n'.join(logger.handlers[0].logs)
    send_message.dingtalk(message_content)
