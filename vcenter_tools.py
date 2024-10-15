import re
import json
import requests

import urllib
import urllib.parse
from pyVim import connect


class Vcenter:
    """
    这个类负责管理与vCenter的连接，并按照一定的格式处理获取到的数据。
    """

    def __init__(self, host, username, password, name):
        self.name = name

        self.host = host
        url = '/api/session'
        response = requests.post(self.host + url, auth=(username, password), verify=False)

        session = re.findall('[0-9a-zA-Z]+', response.text)[0]
        self.headers = {'vmware-api-session-id': session}

        si = connect.SmartConnect(host=host.replace("https://", ""), user=username, pwd=password, port=443,
                                  disableSslCertValidation=True)
        # ServiceInstance.RetrieveContent()  # 检索内容
        self.content = si.content

        self.hosts = []
        for datacenter in self.content.rootFolder.childEntity:
            self.__process_entity(datacenter)

    def __process_entity(self, entity):
        entity_type = str(type(entity))
        if "ComputeResource" in entity_type:
            for HostSystem in entity.host:
                self.hosts.append(HostSystem)
        elif "ClusterComputeResource" in entity_type:
            for HostSystem in entity.host:
                self.hosts.append(HostSystem)
        elif "Datacenter" in entity_type:
            for host in entity.hostFolder.childEntity:
                self.__process_entity(host)
        elif "Folder" in entity_type:
            for host in entity.childEntity:
                self.__process_entity(host)

    def get_datacenter(self) -> list:
        url = '/api/vcenter/datacenter'
        response = requests.get(self.host + url, headers=self.headers, verify=False)
        result = response.json()
        for datacenter in result:
            datacenter["name"] = urllib.parse.unquote(datacenter["name"])

        return result

    def get_host(self, datacenter_id) -> list:
        params = {'datacenters': datacenter_id}
        url = '/api/vcenter/host'
        response = requests.get(self.host + url, params=params, headers=self.headers, verify=False)
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
        response = requests.get(self.host + url, params=params, headers=self.headers, verify=False)
        result = json.loads(response.text)
        for vm in result:
            ipaddress = self.get_vm_ipaddress(vm["vm"])
            vm["ipaddress"] = ipaddress
            uuid = self.get_vm_uuid(vm["vm"])
            vm["uuid"] = uuid
            if "cpu_count" not in vm:
                vm["cpu_count"] = ""
            if "memory_size_MiB" not in vm:
                vm["memory_size_MiB"] = ""
        return result

    def get_vm_uuid(self, vm_id):
        url = '/api/vcenter/vm/%s' % vm_id
        response = requests.get(self.host + url, headers=self.headers, verify=False)
        if "identity" not in response.json():
            return ""
        else:
            return response.json()["identity"]["instance_uuid"]

    def get_vm_ipaddress(self, vm_id):
        url = '/api/vcenter/vm/%s/guest/identity' % vm_id
        response = requests.get(self.host + url, headers=self.headers, verify=False)
        if "ip_address" not in response.json():
            return "0.0.0.0"
        else:
            return response.json()["ip_address"]

    def get_host_uuid(self, ipaddress):
        for HostSystem in self.hosts:
            if HostSystem.name == ipaddress:
                return HostSystem.hardware.systemInfo.uuid

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
