import json
import re
import pymysql
import requests
import urllib.parse
import urllib3
from pyVim import connect
urllib3.disable_warnings()


class Vcenter:
    def __init__(self, host, username, password, name):
        self.name = name

        self.host = host
        url = '/api/session'
        response = requests.post(self.host + url, auth=(username, password), verify=False)

        session = re.findall('[0-9a-zA-Z]+', response.text)[0]
        self.Headers = {'vmware-api-session-id': session}

        self.Hosts = []
        si = connect.SmartConnect(host=host.replace("https://", ""), user=username, pwd=password, port=443, disableSslCertValidation=True)

        # ServiceInstance.RetrieveContent()  # 检索内容
        self.Content = si.content
        for datacenter in self.Content.rootFolder.childEntity:
            if "Datacenter" in str(datacenter):
                self.__process_datacenter(datacenter)
                # print(datacenter)
            elif "Folder" in str(datacenter):
                self.__process_folder(datacenter)
                # print(datacenter)

    def get_datacenter(self) -> list:
        url = '/api/vcenter/datacenter'
        response = requests.get(self.host + url, headers=self.Headers, verify=False)
        result = response.json()
        for datacenter in result:
            datacenter["name"] = urllib.parse.unquote(datacenter["name"])

        return result

    def get_host(self, datacenter_id) -> list:
        params = {'datacenters': datacenter_id}
        url = '/api/vcenter/host'
        response = requests.get(self.host + url, params=params, headers=self.Headers, verify=False)
        result = json.loads(response.text)
        for host in result:
            host["uuid"] = self.get_host_uuid(host["name"])
            if "power_state" not in host:
                host["power_state"] = ""
        return result

    def get_vm(self, host_id) -> list:
        params = {'hosts': host_id
                  # 'power_states': 'POWERED_ON'
                  }
        url = '/api/vcenter/vm'
        response = requests.get(self.host + url, params=params, headers=self.Headers, verify=False)
        result = json.loads(response.text)
        for vm in result:
            ipaddress = self.get_vm_ipaddress(vm["vm"])
            vm["ipaddress"] = ipaddress
            uuid = self.get_vm_uuid(vm["vm"])
            vm["uuid"] = uuid
            if "cpu_count" not in vm:
                vm["cpu_count"] = ""
        return result

    def get_vm_uuid(self, vm_id):
        url = '/api/vcenter/vm/%s' % vm_id
        response = requests.get(self.host + url, headers=self.Headers, verify=False)
        if "identity" not in response.json():
            return ""
        else:
            return response.json()["identity"]["instance_uuid"]

    def get_vm_ipaddress(self, vm_id):
        url = '/api/vcenter/vm/%s/guest/identity' % vm_id
        response = requests.get(self.host + url, headers=self.Headers, verify=False)
        if "ip_address" not in response.json():
            return "0.0.0.0"
        else:
            return response.json()["ip_address"]

    def __process_folder(self, obj):
        for host in obj.childEntity:
            if "ComputeResource" in str(type(host)):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
            elif "ClusterComputeResource" in str(type(host)):
                self.__process_clustercomputeresource(host)
                # print(host)
            elif "Datacenter" in str(host):
                self.__process_datacenter(host)
                # print(host)

    def __process_datacenter(self, obj):
        for host in obj.hostFolder.childEntity:
            # print(host.name)
            if "ClusterComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
                    # print(HostSystem)
            elif "ComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
                    # print(HostSystem)
            elif "Folder" in str(host):
                self.__process_folder(host)
                # print(host)

    def __process_clustercomputeresource(self, obj):
        for HostSystem in obj.host:
            self.Hosts.append(HostSystem)

    def get_host_uuid(self, ipaddress):
        for HostSystem in self.Hosts:
            if HostSystem.name == ipaddress:
                return HostSystem.hardware.systemInfo.uuid

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


class Mysql:

    def __init__(self, host, user, password, database):
        self.db = pymysql.connect(host=host,
                                  user=user,
                                  password=password,
                                  database=database)

    def edit_data(self, sql_statement):
        cursor = self.db.cursor()
        try:
            cursor.execute(sql_statement)
            self.db.commit()
        except:
            self.db.rollback()

    def select_data(self, sql_statement):
        cursor = self.db.cursor()
        cursor.execute(sql_statement)
        result = cursor.fetchall()
        return result


class Process:
    def __init__(self, vcenter_obj):
        self.mysql = Mysql("10.20.120.240", "jiajun.ma", "Abc000425.", "test")
        self.vcenter = vcenter_obj

    def start_sync(self):
        self.__sync_datacenter()
        self.__sync_host()
        self.__sync_virtual_machine()

    def __sync_datacenter(self):
        # 同步数据中心数据
        datacenters = self.vcenter.get_datacenter()
        for datacenter in datacenters:
            exist = self.mysql.select_data(
                f'SELECT * FROM vCenter_datacenter WHERE vc_name="{self.vcenter.name}" AND datacenter_id="{datacenter["datacenter"]}"')
            if not exist:
                self.mysql.edit_data(
                    f'INSERT INTO vCenter_datacenter VALUES ("{self.vcenter.name}","{datacenter["name"]}","{datacenter["datacenter"]}")')

        # 删除vCenter已经不存在的数据
        old_data = self.mysql.select_data(f'SELECT * FROM vCenter_datacenter WHERE vc_name="{self.vcenter.name}"')
        for data in old_data:
            old_data_dict = {"name": data[1], "datacenter": data[2]}
            if old_data_dict not in datacenters:
                self.mysql.edit_data(
                    f'DELETE FROM vCenter_datacenter WHERE vc_name="{self.vcenter.name}" AND datacenter_id="{data[2]}"')
                self.mysql.edit_data(
                    f'INSERT INTO vCenter_datacenter_archive VALUES ("{self.vcenter.name}","{data[1]}","{data[2]}")')

    def __sync_host(self):
        # 同步宿主机数据
        datacenter_tup = self.mysql.select_data(
            f'SELECT datacenter_name, datacenter_id FROM vCenter_datacenter WHERE vc_name="{self.vcenter.name}"')
        for datacenter in datacenter_tup:
            hosts = self.vcenter.get_host(datacenter[1])
            for host in hosts:
                exist = self.mysql.select_data(
                    f'SELECT * FROM vCenter_host WHERE vc_name="{self.vcenter.name}" AND host_id="{host["host"]}" AND host_uuid="{host["uuid"]}" AND host_name="{host["name"]}" AND host_connection_state="{host["connection_state"]}" AND host_power_state="{host["power_state"]}" AND datacenter_name="{datacenter[0]}"')
                if not exist:
                    self.mysql.edit_data(
                        f'INSERT INTO vCenter_host VALUES ("{self.vcenter.name}","{host["host"]}", "{host["uuid"]}", "{host["name"]}","{host["connection_state"]}","{host["power_state"]}","{datacenter[0]}")')

                # 删除vCenter已经不存在的数据
                old_data = self.mysql.select_data(
                    f'SELECT * FROM vCenter_host WHERE vc_name="{self.vcenter.name}" AND datacenter_name="{datacenter[0]}"')
                for data in old_data:
                    old_data_dict = {"host": data[1], "name": data[3], "connection_state": data[4],
                                     "power_state": data[5],
                                     "uuid": data[2]}
                    if old_data_dict not in hosts:
                        self.mysql.edit_data(
                            f'DELETE FROM vCenter_host WHERE vc_name="{self.vcenter.name}" AND host_id="{data[1]}"')
                        self.mysql.edit_data(
                            f'INSERT INTO vCenter_host_archive VALUES ("{self.vcenter.name}","{data[1]}","{data[2]}","{data[3]}","{data[4]}", "{data[5]}", "{datacenter[0]}")')

        # 删除数据中心已不存在的宿主机数据
        old_host_datacenter = set(
            self.mysql.select_data(f'SELECT datacenter_name FROM vCenter_host WHERE vc_name="{self.vcenter.name}"'))
        for host_datacenter in old_host_datacenter:
            # print(host_datacenter[0], [i[0] for i in datacenter_tup])
            if host_datacenter[0] not in [i[0] for i in datacenter_tup]:
                self.mysql.edit_data(f'DELETE FROM vCenter_host WHERE datacenter_name="{host_datacenter[0]}"')

    def __sync_virtual_machine(self):
        # 同步虚拟机数据
        host_tup = self.mysql.select_data(
            f'SELECT host_id, host_name, host_connection_state FROM vCenter_host WHERE vc_name="{self.vcenter.name}" AND host_connection_state="CONNECTED"')
        for host in host_tup:
            vms = self.vcenter.get_vm(host[0])
            # if not host[2] == "DISCONNECTED":  # 已与vCenter断开链接的主机不再添加虚拟机数据
            for vm in vms:
                # ipaddress = self.vcenter.get_vm_info(vm["vm"])
                # ipaddress = None
                exist = self.mysql.select_data(
                    f'SELECT * FROM vCenter_vm WHERE vc_name="{self.vcenter.name}" AND vm_id="{vm["vm"]}" AND vm_uuid="{vm["uuid"]}" AND vm_name="{vm["name"]}" AND vm_ipaddress="{vm["ipaddress"]}" AND vm_power_state="{vm["power_state"]}" AND vm_cpu_count={vm["cpu_count"]} AND vm_memory_size_MiB={vm["memory_size_MiB"]} AND host_name="{host[1]}"')
                print(exist)
                if not exist:
                    self.mysql.edit_data(
                        f'INSERT INTO vCenter_vm VALUES ("{self.vcenter.name}","{vm["vm"]}","{vm["uuid"]}","{vm["name"]}","{vm["ipaddress"]}","{vm["power_state"]}",{vm["cpu_count"]},{vm["memory_size_MiB"]},"{host[1]}")')

            # 删除vCenter已经不存在的数据
            old_data = self.mysql.select_data(
                f'SELECT * FROM vCenter_vm WHERE vc_name="{self.vcenter.name}" AND host_name="{host[1]}"')
            for data in old_data:
                old_data_dict = {"memory_size_MiB": data[7], "vm": data[1], "name": data[3], "power_state": data[5],
                                 "cpu_count": data[6], "ipaddress": data[4], "uuid": data[2]}
                if old_data_dict not in vms:
                    self.mysql.edit_data(
                        f'DELETE FROM vCenter_vm WHERE vc_name="{self.vcenter.name}" AND vm_id="{data[1]}"')
                    self.mysql.edit_data(
                        f'INSERT INTO vCenter_vm_archive VALUES ("{self.vcenter.name}","{data[1]}","{data[2]}","{data[3]}","{data[4]}","{data[5]}",{data[6]},{data[7]},"{host[1]}")')

        # 删除宿主机已不存在的虚拟机数据
        old_vm = set(self.mysql.select_data(f'SELECT host_name FROM vCenter_vm WHERE vc_name="{self.vcenter.name}"'))
        for vm_host in old_vm:
            if vm_host[0] not in [i[1] for i in host_tup]:
                self.mysql.edit_data(f'DELETE FROM vCenter_vm WHERE host_name="{vm_host[0]}"')
