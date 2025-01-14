#!/usr/bin/python3
import json
import urllib3
import base64
import lzma
import concurrent.futures
import subprocess
import tempfile
import os
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
        host_futures = {
            executor.submit(vcenter_obj.get_host, dc["datacenter"]): dc
            for dc in datacenter_data
        }
        for future in concurrent.futures.as_completed(host_futures):
            datacenter = host_futures[future]
            try:
                hosts = future.result()
                for host in hosts:
                    host["datacenter_name"] = datacenter["name"]
                host_data.extend(hosts)

                # 并行获取每个主机的虚拟机信息
                vm_futures = {
                    executor.submit(vcenter_obj.get_vm, host["host"]): host
                    for host in hosts
                }
                for vm_future in concurrent.futures.as_completed(vm_futures):
                    try:
                        vms = vm_future.result()
                        host_info = vm_futures[vm_future]
                        for vm in vms:
                            vm["host_name"] = host_info["name"]
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
    """
    将数据转为 JSON 后用 LZMA 压缩，并进行 base64 编码。
    """
    json_data = json.dumps(data, separators=(',', ':')).encode('utf-8')
    compressed_data = lzma.compress(json_data, preset=9 | lzma.PRESET_EXTREME)
    base64_encoded_data = base64.b64encode(compressed_data).decode('utf-8')
    return base64_encoded_data

def send_to_zabbix(zabbix_proxy, host, key, value):
    """
    调用 zabbix_sender 命令，将数据发送到指定的 Zabbix Proxy。
    - zabbix_proxy: Zabbix Proxy 或 Server 地址
    - host: Zabbix 上配置的主机名（需提前在 Zabbix 中创建）
    - key: 监控项 key（需在 Zabbix 中匹配）
    - value: 要发送的值
    """
    # 通过临时文件的方式发送
    with tempfile.NamedTemporaryFile('w', delete=False) as tmpfile:
        tmpfile_name = tmpfile.name
        # zabbix_sender 的文件格式：hostname key timestamp value
        # timestamp 可留空，默认就是当前时间
        tmpfile.write(f"{host} {key} {value}\n")

    try:
        subprocess.run(
            ["zabbix_sender", "-z", zabbix_proxy, "-i", tmpfile_name],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"Data sent to Zabbix Proxy {zabbix_proxy} for host '{host}' key '{key}' successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error sending data to Zabbix:\n{e.stderr.decode('utf-8')}")
    finally:
        os.remove(tmpfile_name)

def main():
    """
    这里演示如何将多个 vCenter 的数据推送到不同监控项。
    你需要在 Zabbix 中为每个 vCenter 主机分别配置监控项 key。
    """

    # 你的 Zabbix Proxy (或 Zabbix Server) 地址
    zabbix_proxy = "192.168.101.4"

    # 定义多个 vCenter 证书信息，每个 vCenter 对应一个不同的 Zabbix Key
    certificates = [
        {
            "name": "IT中心物理内网VCenter",
            "host": "https://192.168.100.56",
            "username": "administrator@vsphere.local",
            "password": "U33@*pYau3zXKty8",
            "zabbix_key": "vmware.data.nwvcenter"
        },
        {
            "name": "IT中心物理内网云桌面VCenter",
            "host": "https://192.168.200.2",
            "username": "administrator@vsphere.local",
            "password": "jh29+$9B$B5F~gmW",
            "zabbix_key": "vmware.data.vd-vc"
        }
    ]

    for certificate in certificates:
        data = get_vcenter_data([
            certificate["name"],
            certificate["host"],
            certificate["username"],
            certificate["password"]
        ])
        # 将本 vCenter 的数据封装到字典中
        all_data_for_this_vcenter = {
            "vcenter": certificate["name"],
            "data": data
        }

        # 压缩数据
        compressed_output = compress_data(all_data_for_this_vcenter)

        # 发送至 Zabbix
        send_to_zabbix(
            zabbix_proxy=zabbix_proxy,
            host="503be708-00ce-7aad-6fd4-677fd897bdd0",  # 对应 Zabbix 上的主机名
            key=certificate["zabbix_key"],
            value=compressed_output
        )

if __name__ == '__main__':
    main()
