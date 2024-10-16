import logging
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


def get_zabbix_data(item_names):
    # 从 Zabbix API 获取指定监控项的最新数据
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
                "name": item_names
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
        self.data = []
        for item in data:
            if isinstance(item, list):
                self.data.extend(item)
            else:
                self.data.append(item)
        # 获取当前处理的 vc_name 列表
        self.vc_names = set()
        for vcenter_data in self.data:
            vc_name = vcenter_data.get("vcenter")
            if vc_name:
                self.vc_names.add(vc_name)

    def start_sync(self):
        self.__sync_datacenter()
        self.__sync_host()
        self.__sync_virtual_machine()
        self.conn.commit()  # 确保在每个数据块处理完后提交事务

    def __sync_datacenter(self):
        # 同步数据中心数据
        datacenter_records = []
        for vcenter_data in self.data:
            vc_name = vcenter_data.get("vcenter")
            datacenters = vcenter_data.get("data", {}).get("datacenter", [])
            for datacenter in datacenters:
                datacenter_records.append((
                    vc_name,
                    datacenter["datacenter"],
                    datacenter["name"]
                ))
        # 批量插入或更新数据中心
        self.__upsert_datacenters(datacenter_records)

    def __upsert_datacenters(self, records):
        for record in records:
            vc_name, datacenter_id, datacenter_name = record
            self.pgsql.execute(
                'SELECT datacenter_name FROM "vCenter_datacenter" WHERE vc_name=%s AND datacenter_id=%s',
                (vc_name, datacenter_id))
            exist = self.pgsql.fetchone()
            if not exist:
                # 插入新的数据中心
                self.pgsql.execute(
                    'INSERT INTO "vCenter_datacenter" (vc_name, datacenter_name, datacenter_id) VALUES (%s, %s, %s)',
                    (vc_name, datacenter_name, datacenter_id))
            else:
                # 更新数据中心名称（如果已更改）
                if exist[0] != datacenter_name:
                    self.pgsql.execute(
                        'UPDATE "vCenter_datacenter" SET datacenter_name=%s WHERE vc_name=%s AND datacenter_id=%s',
                        (datacenter_name, vc_name, datacenter_id))
        # 删除已不存在的数据中心
        self.__cleanup_datacenters(records)

    def __cleanup_datacenters(self, current_records):
        current_set = set((r[0], r[1]) for r in current_records)
        # 只查询当前 vc_name 下的数据
        for vc_name in self.vc_names:
            self.pgsql.execute(
                'SELECT vc_name, datacenter_id, datacenter_name FROM "vCenter_datacenter" WHERE vc_name=%s', (vc_name,))
            all_records = self.pgsql.fetchall()
            for record in all_records:
                if (record[0], record[1]) not in current_set:
                    # 删除并归档
                    self.pgsql.execute(
                        'DELETE FROM "vCenter_datacenter" WHERE vc_name=%s AND datacenter_id=%s',
                        (record[0], record[1]))
                    self.pgsql.execute(
                        'INSERT INTO "vCenter_datacenter_archive" (vc_name, datacenter_name, datacenter_id) VALUES (%s, %s, %s)',
                        (record[0], record[2], record[1]))

    def __sync_host(self):
        # 同步宿主机数据
        host_records = []
        for vcenter_data in self.data:
            vc_name = vcenter_data.get("vcenter")
            hosts = vcenter_data.get("data", {}).get("hosts", [])
            for host in hosts:
                host_records.append((
                    vc_name,
                    host["host"],
                    host["uuid"],
                    host["name"],
                    host["connection_state"],
                    host["power_state"],
                    host["datacenter_name"]
                ))
        # 批量插入或更新宿主机
        self.__upsert_hosts(host_records)

    def __upsert_hosts(self, records):
        for record in records:
            vc_name, host_id, host_uuid, host_name, conn_state, power_state, datacenter_name = record
            self.pgsql.execute(
                'SELECT host_uuid, host_name, host_connection_state, host_power_state, datacenter_name FROM "vCenter_host" WHERE vc_name=%s AND host_id=%s',
                (vc_name, host_id))
            exist = self.pgsql.fetchone()
            if not exist:
                # 插入新的宿主机
                self.pgsql.execute(
                    'INSERT INTO "vCenter_host" (vc_name, host_id, host_uuid, host_name, host_connection_state, host_power_state, datacenter_name) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (vc_name, host_id, host_uuid, host_name, conn_state, power_state, datacenter_name))
            else:
                # 更新宿主机信息（如果有变化）
                if (exist[0] != host_uuid or exist[1] != host_name or exist[2] != conn_state or
                        exist[3] != power_state or exist[4] != datacenter_name):
                    self.pgsql.execute(
                        'UPDATE "vCenter_host" SET host_uuid=%s, host_name=%s, host_connection_state=%s, host_power_state=%s, datacenter_name=%s WHERE vc_name=%s AND host_id=%s',
                        (host_uuid, host_name, conn_state, power_state, datacenter_name, vc_name, host_id))
        # 删除已不存在的宿主机
        self.__cleanup_hosts(records)

    def __cleanup_hosts(self, current_records):
        current_set = set((r[0], r[1]) for r in current_records)
        # 只查询当前 vc_name 下的数据
        for vc_name in self.vc_names:
            self.pgsql.execute(
                'SELECT vc_name, host_id, host_uuid, host_name, host_connection_state, host_power_state, datacenter_name FROM "vCenter_host" WHERE vc_name=%s',
                (vc_name,))
            all_records = self.pgsql.fetchall()
            for record in all_records:
                if (record[0], record[1]) not in current_set:
                    # 删除并归档
                    self.pgsql.execute(
                        'DELETE FROM "vCenter_host" WHERE vc_name=%s AND host_id=%s',
                        (record[0], record[1]))
                    self.pgsql.execute(
                        'INSERT INTO "vCenter_host_archive" (vc_name, host_id, host_uuid, host_name, host_connection_state, host_power_state, datacenter_name) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                        (record[0], record[1], record[2], record[3], record[4], record[5], record[6]))

    def __sync_virtual_machine(self):
        # 同步虚拟机数据
        vm_records = []
        for vcenter_data in self.data:
            vc_name = vcenter_data.get("vcenter")
            vms = vcenter_data.get("data", {}).get("vms", [])
            for vm in vms:
                vm_records.append((
                    vc_name,
                    vm["vm"],
                    vm["uuid"],
                    vm["name"],
                    vm["ipaddress"],
                    vm["power_state"],
                    vm["cpu_count"],
                    vm["memory_size_MiB"],
                    vm["host_name"]
                ))
        # 批量插入或更新虚拟机
        self.__upsert_vms(vm_records)

    def __upsert_vms(self, records):
        for record in records:
            vc_name, vm_id, vm_uuid, vm_name, ipaddress, power_state, cpu_count, memory_size, host_name = record

            if not cpu_count:
                cpu_count = 0
            if not memory_size:
                memory_size = 0

            self.pgsql.execute(
                'SELECT vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name FROM "vCenter_vm" WHERE vc_name=%s AND vm_id=%s',
                (vc_name, vm_id))
            exist = self.pgsql.fetchone()
            if not exist:
                # 插入新的虚拟机
                self.pgsql.execute(
                    'INSERT INTO "vCenter_vm" (vc_name, vm_id, vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (vc_name, vm_id, vm_uuid, vm_name, ipaddress, power_state, cpu_count, memory_size, host_name))
            else:
                # 更新虚拟机信息（如果有变化）
                if (exist[0] != vm_uuid or exist[1] != vm_name or exist[2] != ipaddress or exist[3] != power_state or
                        exist[4] != cpu_count or exist[5] != memory_size or exist[6] != host_name):
                    self.pgsql.execute(
                        'UPDATE "vCenter_vm" SET vm_uuid=%s, vm_name=%s, vm_ipaddress=%s, vm_power_state=%s, vm_cpu_count=%s, "vm_memory_size_MiB"=%s, host_name=%s WHERE vc_name=%s AND vm_id=%s',
                        (vm_uuid, vm_name, ipaddress, power_state, cpu_count, memory_size, host_name, vc_name, vm_id))
        # 删除已不存在的虚拟机
        self.__cleanup_vms(records)

    def __cleanup_vms(self, current_records):
        current_set = set((r[0], r[1]) for r in current_records)
        # 只查询当前 vc_name 下的数据
        for vc_name in self.vc_names:
            self.pgsql.execute(
                'SELECT vc_name, vm_id, vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name FROM "vCenter_vm" WHERE vc_name=%s',
                (vc_name,))
            all_records = self.pgsql.fetchall()
            for record in all_records:
                if (record[0], record[1]) not in current_set:
                    # 删除并归档
                    self.pgsql.execute(
                        'DELETE FROM "vCenter_vm" WHERE vc_name=%s AND vm_id=%s',
                        (record[0], record[1]))
                    self.pgsql.execute(
                        'INSERT INTO "vCenter_vm_archive" (vc_name, vm_id, vm_uuid, vm_name, vm_ipaddress, vm_power_state, vm_cpu_count, "vm_memory_size_MiB", host_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7],
                         record[8]))

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

    # 指定要获取的监控项名称列表
    item_names = ["vd-vc数据采集", "nwvcenter数据采集"]
    # 从Zabbix API读取数据
    data = get_zabbix_data(item_names)

    if not data:
        logger.error("No data retrieved from Zabbix.")
        return

    # 使用多进程来加速数据处理
    num_processes = min(len(data), multiprocessing.cpu_count())
    chunk_size = max(1, len(data) // num_processes)
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
    logger.info("物理内网vCenter数据同步到PostgreSQL执行完成。耗时：%.1f 秒" % process_time)


if __name__ == '__main__':
    main()
