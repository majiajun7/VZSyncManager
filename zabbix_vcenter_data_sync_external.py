import json
import time

import psycopg2
import multiprocessing
import requests
import base64
import lzma
from log_handler import get_logger

logger = get_logger(__name__)

ZABBIX_API_URL = "http://10.20.120.239/api_jsonrpc.php"
ZABBIX_API_USER = "Admin"
ZABBIX_API_PASSWORD = "M3rxQQzJr2iNe!2rkSrf"


def get_zabbix_data():
    # 从 Zabbix API 获取监控项名称为 "vd-vc数据采集" 的最新数据
    headers = {'Content-Type': 'application/json-rpc'}
    auth_payload = {
        "jsonrpc": "2.0",
        "method": "user.login",
        "params": {
            "user": ZABBIX_API_USER,
            "password": ZABBIX_API_PASSWORD
        },
        "id": 1
    }
    auth_response = requests.post(ZABBIX_API_URL, headers=headers, json=auth_payload)
    auth_token = auth_response.json().get("result")

    if not auth_token:
        logger.error("Failed to authenticate to Zabbix API.")
        return []

    item_payload = {
        "jsonrpc": "2.0",
        "method": "item.get",
        "params": {
            "output": ["itemid", "name", "lastvalue"],
            "filter": {
                "name": "vd-vc数据采集"
            },
            "sortfield": "name"
        },
        "auth": auth_token,
        "id": 2
    }
    item_response = requests.post(ZABBIX_API_URL, headers=headers, json=item_payload)
    items = item_response.json().get("result", [])

    data = []
    for item in items:
        try:
            # 解码并解压Zabbix中的压缩数据
            compressed_data = base64.b64decode(item["lastvalue"])
            json_data = lzma.decompress(compressed_data).decode('utf-8')
            item_data = json.loads(json_data)
            data.append(item_data)
        except (json.JSONDecodeError, lzma.LZMAError, base64.binascii.Error) as e:
            logger.error(f"Failed to decode JSON for item: {item['name']}, error: {e}")

    return data


class DataProcess:
    def __init__(self, data):
        self.conn = psycopg2.connect(f"host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
        self.pgsql = self.conn.cursor()
        self.data = data

    def start_sync(self):
        self.__sync_datacenter()
        self.__sync_host()
        self.__sync_virtual_machine()
        self.conn.commit()  # 确保在每个数据块处理完后提交事务

    def __sync_datacenter(self):
        # 同步数据中心数据
        for vcenter_data in self.data:
            if isinstance(vcenter_data, list):
                for single_data in vcenter_data:
                    self.__sync_single_datacenter(single_data)
            else:
                self.__sync_single_datacenter(vcenter_data)

    def __sync_single_datacenter(self, vcenter_data):
        vc_name = vcenter_data.get("vcenter")
        datacenters = vcenter_data.get("data", {}).get("datacenter", [])
        datacenter_list = []

        for datacenter in datacenters:
            datacenter_list.append({"datacenter_id": datacenter["datacenter"], "name": datacenter["name"]})
            # 检查数据中心是否存在
            self.pgsql.execute(
                'SELECT datacenter_name FROM "vCenter_datacenter" WHERE vc_name=%s AND datacenter_id=%s',
                (vc_name, datacenter["datacenter"]))
            exist = self.pgsql.fetchone()
            if not exist:
                # 插入新的数据中心
                self.pgsql.execute(
                    'INSERT INTO "vCenter_datacenter" (vc_name, datacenter_name, datacenter_id) VALUES (%s, %s, %s)',
                    (vc_name, datacenter["name"], datacenter["datacenter"]))
            else:
                # 更新数据中心名称（如果已更改）
                if exist[0] != datacenter["name"]:
                    self.pgsql.execute(
                        'UPDATE "vCenter_datacenter" SET datacenter_name=%s WHERE vc_name=%s AND datacenter_id=%s',
                        (datacenter["name"], vc_name, datacenter["datacenter"]))

        # 删除已不存在的数据中心
        self.pgsql.execute('SELECT datacenter_name, datacenter_id FROM "vCenter_datacenter" WHERE vc_name=%s', (vc_name,))
        old_data = self.pgsql.fetchall()
        old_datacenters = [{"datacenter_id": data[1], "name": data[0]} for data in old_data]
        for data in old_datacenters:
            if data not in datacenter_list:
                # 删除并归档
                self.pgsql.execute(
                    'DELETE FROM "vCenter_datacenter" WHERE vc_name=%s AND datacenter_id=%s',
                    (vc_name, data["datacenter_id"]))
                self.pgsql.execute(
                    'INSERT INTO "vCenter_datacenter_archive" (vc_name, datacenter_name, datacenter_id) VALUES (%s, %s, %s)',
                    (vc_name, data["name"], data["datacenter_id"]))

    def __sync_host(self):
        # 同步宿主机数据
        for vcenter_data in self.data:
            if isinstance(vcenter_data, list):
                for single_data in vcenter_data:
                    self.__sync_single_host(single_data)
            else:
                self.__sync_single_host(vcenter_data)

    def __sync_single_host(self, vcenter_data):
        vc_name = vcenter_data.get("vcenter")

        hosts = vcenter_data.get("data", {}).get("hosts", [])
        host_list = []

        for host in hosts:
            host_data = {
                "host_id": host["host"],
                "host_uuid": host["uuid"],
                "host_name": host["name"],
                "host_connection_state": host["connection_state"],
                "host_power_state": host["power_state"],
                "datacenter_name": host["datacenter_name"]
            }
            host_list.append(host_data)

            # 检查宿主机是否存在
            self.pgsql.execute(
                'SELECT host_uuid, host_name, host_connection_state, host_power_state, datacenter_name FROM "vCenter_host" WHERE vc_name=%s AND host_id=%s',
                (vc_name, host["host"]))
            result = self.pgsql.fetchone()
            if result:
                # 宿主机存在，检查是否需要更新
                if (result[0] != host["uuid"] or result[1] != host["name"] or
                    result[2] != host["connection_state"] or result[3] != host["power_state"] or
                    result[4] != host["datacenter_name"]):
                    # 更新宿主机信息
                    self.pgsql.execute(
                        'UPDATE "vCenter_host" SET host_uuid=%s, host_name=%s, host_connection_state=%s, host_power_state=%s, datacenter_name=%s WHERE vc_name=%s AND host_id=%s',
                        (host["uuid"], host["name"], host["connection_state"], host["power_state"], host["datacenter_name"], vc_name, host["host"]))
            else:
                # 插入新的宿主机
                self.pgsql.execute(
                    'INSERT INTO "vCenter_host" (vc_name, host_id, host_uuid, host_name, host_connection_state, host_power_state, datacenter_name) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (vc_name, host["host"], host["uuid"], host["name"], host["connection_state"], host["power_state"], host["datacenter_name"]))

        # 删除已不存在的宿主机
        self.pgsql.execute('SELECT host_id FROM "vCenter_host" WHERE vc_name=%s', (vc_name,))
        old_hosts = self.pgsql.fetchall()
        old_host_ids = set([h[0] for h in old_hosts])
        current_host_ids = set([h["host_id"] for h in host_list])

        hosts_to_delete = old_host_ids - current_host_ids

        for host_id in hosts_to_delete:
            # 获取宿主机详情以进行归档
            self.pgsql.execute(
                'SELECT host_uuid, host_name, host_connection_state, host_power_state, datacenter_name FROM "vCenter_host" WHERE vc_name=%s AND host_id=%s',
                (vc_name, host_id))
            data = self.pgsql.fetchone()
            # 删除并归档
            self.pgsql.execute(
                'DELETE FROM "vCenter_host" WHERE vc_name=%s AND host_id=%s',
                (vc_name, host_id))
            self.pgsql.execute(
                'INSERT INTO "vCenter_host_archive" (vc_name, host_id, host_uuid, host_name, host_connection_state, host_power_state, datacenter_name) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (vc_name, host_id, data[0], data[1], data[2], data[3], data[4]))

    def __sync_virtual_machine(self):
        # 同步虚拟机数据
        for vcenter_data in self.data:
            if isinstance(vcenter_data, list):
                for single_data in vcenter_data:
                    self.__sync_single_virtual_machine(single_data)
            else:
                self.__sync_single_virtual_machine(vcenter_data)

    def __sync_single_virtual_machine(self, vcenter_data):
        vc_name = vcenter_data.get("vcenter")
        vms = vcenter_data.get("data", {}).get("vms", [])
        vm_list = []

        for vm in vms:
            vm_data = {
                "vm_id": vm["vm"],
                "vm_uuid": vm["uuid"],
                "vm_name": vm["name"],
                "vm_ipaddress": vm["ipaddress"],
                "vm_power_state": vm["power_state"],
                "vm_cpu_count": vm["cpu_count"],
                "vm_memory_size_MiB": vm["memory_size_MiB"],
                "host_name": vm["host_name"]
            }
            vm_list.append(vm_data)

            # 检查虚拟机是否存在
            self.pgsql.execute(
                'SELECT vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name FROM "vCenter_vm" WHERE vc_name=%s AND vm_id=%s',
                (vc_name, vm["vm"]))
            result = self.pgsql.fetchone()
            if result:
                # 虚拟机存在，检查是否需要更新
                if (result[0] != vm["uuid"] or result[1] != vm["name"] or result[2] != vm["ipaddress"] or
                    result[3] != vm["power_state"] or result[4] != vm["cpu_count"] or result[5] != vm["memory_size_MiB"] or
                    result[6] != vm["host_name"]):
                    # 更新虚拟机信息
                    self.pgsql.execute(
                        'UPDATE "vCenter_vm" SET vm_uuid=%s, vm_name=%s, vm_ipaddress=%s, vm_power_state=%s, vm_cpu_count=%s, "vm_memory_size_MiB"=%s, host_name=%s WHERE vc_name=%s AND vm_id=%s',
                        (vm["uuid"], vm["name"], vm["ipaddress"], vm["power_state"], vm["cpu_count"], vm["memory_size_MiB"], vm["host_name"], vc_name, vm["vm"]))
            else:
                # 插入新的虚拟机
                self.pgsql.execute(
                    'INSERT INTO "vCenter_vm" (vc_name, vm_id, vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (vc_name, vm["vm"], vm["uuid"], vm["name"], vm["ipaddress"], vm["power_state"], vm["cpu_count"], vm["memory_size_MiB"], vm["host_name"]))

        # 删除已不存在的虚拟机
        self.pgsql.execute('SELECT vm_id FROM "vCenter_vm" WHERE vc_name=%s', (vc_name,))
        old_vms = self.pgsql.fetchall()
        old_vm_ids = set([v[0] for v in old_vms])
        current_vm_ids = set([v["vm_id"] for v in vm_list])

        vms_to_delete = old_vm_ids - current_vm_ids

        for vm_id in vms_to_delete:
            # 获取虚拟机详情以进行归档
            self.pgsql.execute(
                'SELECT vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name FROM "vCenter_vm" WHERE vc_name=%s AND vm_id=%s',
                (vc_name, vm_id))
            data = self.pgsql.fetchone()
            # 删除并归档
            self.pgsql.execute(
                'DELETE FROM "vCenter_vm" WHERE vc_name=%s AND vm_id=%s',
                (vc_name, vm_id))
            self.pgsql.execute(
                'INSERT INTO "vCenter_vm_archive" (vc_name, vm_id, vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (vc_name, vm_id, data[0], data[1], data[2], data[3], data[4], data[5], data[6]))

    def __del__(self):
        self.conn.commit()
        self.pgsql.close()
        self.conn.close()


def start_process(data_chunk):
    run = DataProcess(data_chunk)
    run.start_sync()


def main():
    logger.info("开始同步物理内网的vCenter数据到PostgreSQL。")
    start_time = time.time()

    # 从Zabbix API读取数据
    data = get_zabbix_data()

    if not data:
        logger.error("No data retrieved from Zabbix.")
        return

    # 使用多进程来加速数据处理
    chunk_size = max(1, len(data) // 4)  # 将数据分成4份，以便使用4个进程
    processes = []
    for i in range(0, len(data), chunk_size):
        data_chunk = data[i:i + chunk_size]
        process = multiprocessing.Process(target=start_process, args=(data_chunk,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    end_time = time.time()
    process_time = end_time - start_time
    logger.info("物理内网vCenter数据到PostgreSQL执行完成。耗时：%d 秒" % process_time)


if __name__ == '__main__':
    main()
