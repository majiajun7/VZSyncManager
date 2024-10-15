#!/usr/bin/python3
import json
import urllib3
import gzip
import base64
import zlib
import lzma
import concurrent.futures
from vcenter_tools import Vcenter

urllib3.disable_warnings()


def get_vcenter_data(certificate):
    vcenter_obj = Vcenter(certificate[1], certificate[2], certificate[3], certificate[0])

    # 使用多线程来并行获取数据中心、主机和虚拟机的数据，以加快速度
    with concurrent.futures.ThreadPoolExecutor() as executor:
        datacenter_future = executor.submit(vcenter_obj.get_datacenter)
        host_data = []
        vm_data = []
        datacenter_data = datacenter_future.result()

        # 并行获取所有数据中心的主机信息
        host_futures = {executor.submit(vcenter_obj.get_host, datacenter["datacenter"]): datacenter for datacenter in
                        datacenter_data}
        for future in concurrent.futures.as_completed(host_futures):
            datacenter = host_futures[future]
            try:
                hosts = future.result()
                for host in hosts:
                    host['datacenter_name'] = datacenter['name']
                host_data.extend(hosts)

                # 并行获取每个主机的虚拟机信息
                vm_futures = {executor.submit(vcenter_obj.get_vm, host["host"]): host for host in hosts}
                for vm_future in concurrent.futures.as_completed(vm_futures):
                    try:
                        vms = vm_future.result()
                        for vm in vms:
                            vm['host_name'] = host['name']
                        vm_data.extend(vms)
                    except Exception as e:
                        print(f"Error getting VMs for host {vm_futures[vm_future]['host']}: {e}")
            except Exception as e:
                print(f"Error getting hosts for datacenter {datacenter['datacenter']}: {e}")

    return {
        "datacenter": datacenter_data,
        "hosts": host_data,
        "vms": vm_data
    }


def compress_data(data):
    json_data = json.dumps(data, separators=(',', ':')).encode('utf-8')
    compressed_data = lzma.compress(json_data, preset=9 | lzma.PRESET_EXTREME)
    base64_encoded_data = base64.b64encode(compressed_data).decode('utf-8')
    return base64_encoded_data


def main():
    certificates = [
        {
            "name": "nwvcenter",
            "host": "https://192.168.100.56",
            "username": "administrator@vsphere.local",
            "password": "U33@*pYau3zXKty8"
        }
    ]

    all_data = []
    for certificate in certificates:
        data = get_vcenter_data(
            [certificate["name"], certificate["host"], certificate["username"], certificate["password"]])
        all_data.append({"vcenter": certificate["name"], "data": data})

    # 压缩并编码输出为JSON格式，供zabbix监控项读取
    compressed_output = compress_data(all_data)
    print(compressed_output)


if __name__ == '__main__':
    main()
