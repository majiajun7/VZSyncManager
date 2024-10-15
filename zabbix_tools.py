import json
import re

import requests


class Zabbix:
    def __init__(self):
        self.url = "http://10.20.120.239/api_jsonrpc.php"
        self.header = {"Content-Type": "application/json-rpc"}
        data = {
            "jsonrpc": "2.0",
            "method": "user.login",
            "params": {
                "user": "jiajun.ma",
                "password": "Abc000425."
            },
            "id": "1",
            "auth": None}
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        self.session = response.json()["result"]

        self.__load_host_metadata()

    def __load_host_metadata(self):
        data = {
            "jsonrpc": "2.0",
            "method": "host.get",
            "params": {
                "selectMacros": "extend",
                "selectInterfaces": "extend",
                "selectGroups": "extend"
            },
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        self.metadata = response.json()

    def check_host_exist(self, uuid, name):
        for host in self.metadata["result"]:
            # 遍历所有主机，匹配宏内是否存在虚拟机UUID
            for macro in host["macros"]:
                if macro["value"] == uuid:
                    return host
            # 检查宿主机IP是否存在于zabbix的主机可见名称中
            # 检测前置条件：值必须为IP格式。
            if "10." in host['name']:
                if re.search("%s$" % re.escape(host['name']), name):
                    return host

        return False

    def check_vm_host_exist(self, uuid):
        for host in self.metadata["result"]:
            for macro in host["macros"]:
                if macro["value"] == uuid:  # 检测zabbix中主机是否存在的逻辑：遍历所有主机，匹配宏内是否存在虚拟机UUID
                    return host

        return False
    def create_host(self, name, displayname, group_id, template_id=None, interface=None, macros=None):
        data = {
            "jsonrpc": "2.0",
            "method": "host.create",
            "params": {
                "host": name,
                "name": displayname,
                "groups": [
                    {
                        "groupid": group_id
                    }
                ],

            },
            "auth": self.session,
            "id": 1
        }
        if interface:
            data["params"]["interfaces"] = interface
        if macros:
            data["params"]["macros"] = macros
        if template_id:
            data["params"]["templates"] = [{"templateid": template_id}]
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        # print(response.text)

    def create_host_group(self, name):
        data = {
            "jsonrpc": "2.0",
            "method": "hostgroup.create",
            "params": {
                "name": name
            },
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        return response.json()

    def update_host(self, hostid, displayname=None, macro=None, group_list=None, name=None, interface=None):
        data = {
            "jsonrpc": "2.0",
            "method": "host.update",
            "params": {
                "hostid": hostid
            },
            "auth": self.session,
            "id": 1
        }
        if displayname:
            data["params"]["name"] = displayname
        if macro:
            data["params"]["macros"] = macro
        if group_list:
            data["params"]["groups"] = group_list
        if name:
            data["params"]["host"] = name
        if interfaces:
            data["params"]["interfaces"] = interfaces


        response = requests.post(self.url, json.dumps(data), headers=self.header)
        print(response.text)
        print(displayname, "???????")

    def get_host_id(self, hostname=None, serial_number=None):
        if hostname:
            data = {
                "jsonrpc": "2.0",
                "method": "host.get",
                "params": {
                    "output": "extend"
                },
                "auth": self.session,
                "id": 1
            }
            response = requests.post(self.url, json.dumps(data), headers=self.header)
            for host in response.json()["result"]:
                if host["name"] == hostname:
                    return host["hostid"]
        if serial_number:
            data = {
                "jsonrpc": "2.0",
                "method": "item.get",
                "params": {
                    "output": "extend",
                    "filter": {
                        "name": "Hardware serial number",
                        "state": "0"
                    }
                },
                "auth": self.session,
                "id": 1
            }
            response = requests.post(self.url, json.dumps(data), headers=self.header)
            for item in response.json()["result"]:
                if item["lastvalue"] == serial_number:
                    return item["hostid"]

    def delete_host(self, host_id):
        data = {
            "jsonrpc": "2.0",
            "method": "host.delete",
            "params": [
                host_id
            ],
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        # print(response.text)

    def get_host(self):
        data = {
            "jsonrpc": "2.0",
            "method": "host.get",
            "params": {},
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        return response.json()

    def get_host_group_all(self):
        data = {
            "jsonrpc": "2.0",
            "method": "hostgroup.get",
            "params": {
                "output": "extend"
            },
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        return response.json()

    # def get_host_group(self, host_id):
    #     data = {
    #         "jsonrpc": "2.0",
    #         "method": "host.get",
    #         "params": {
    #             "selectGroups" : "extend",
    #             "hostids" : host_id
    #         },
    #         "auth": self.session,
    #         "id": 1
    #     }
    #     response = requests.post(self.url, json.dumps(data), headers=self.header)
    #     return response.json()['result'][0]['groups']

    def update_host_group(self, group_id, group_name):
        data = {
            "jsonrpc": "2.0",
            "method": "hostgroup.update",
            "params": {
                "groupid": group_id,
                "name": group_name
            },
            "auth": self.session,
            "id": 1
        }
        response = requests.post(self.url, json.dumps(data), headers=self.header)
        # print(response.text)
        return response.json()

    def get_inventory(self, serial_number):
        host_id = self.get_host_id(serial_number=serial_number)
        if host_id:
            inventory = ['VMware: CPU cores', 'VMware: Total memory']

if __name__ == '__main__':
    def override_hosts():
        """遍历文本中要添加的主机信息，主机存在则关联指定模板，不存在则新建主机并关联指定模板和群组"""
        run = Zabbix()
        for info in open("zabbix.txt", "r", encoding="utf8").read().split("\n"):
            ip, name = info.split("\t")
            result = run.check_host_exist(ip)
            print(result)
            if result:
                run.update_host(result)
            else:
                run.create_host(ip, name, ip, "5", "10118")


    def add_hosts():
        """遍历文本中要添加的主机信息，新建主机并关联指定模板和群组"""
        run = Zabbix()
        for info in open("zabbix.txt", "r", encoding="utf8").read().split("\n"):
            ip, name = info.split()
            interface = [
                {
                    "type": 2,
                    "main": 1,
                    "useip": 1,
                    "ip": ip,
                    "dns": "",
                    "port": "161",
                    "details": {
                        "version": 2,
                        "bulk": 1,
                        "community": "{$SNMP_COMMUNITY}",
                    }
                }
            ]
            run.create_host(ip, name + '-' + ip, "461", "17159", interface)


    def update_hosts():
        """将主机的可见名称清除"""
        run = Zabbix()
        content = json.loads(open("zabbix_network.txt", "r", encoding="utf8").read())
        host_id_list = [[i["hostid"], i["host"]] for i in content["result"]]
        for a in host_id_list:
            run.update_host(a[0], a[1])


    def delete_hosts():
        """匹配文本中的主机可见名称并删除"""
        run = Zabbix()
        for name in open("zabbix_delete.txt", "r", encoding="utf8").read().split("\n"):
            host_id = run.get_host_id(name)
            run.delete_host(host_id)


    add_hosts()
