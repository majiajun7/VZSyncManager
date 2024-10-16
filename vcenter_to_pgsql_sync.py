import multiprocessing
import time

import psycopg
import urllib3

from vcenter_tools import Vcenter
from log_handler import get_logger

urllib3.disable_warnings()

logger = get_logger(__name__)


class DataProcess:
    def __init__(self, vcenter_obj):
        self.conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
        self.pgsql = self.conn.cursor()
        self.vcenter = vcenter_obj

    def start_sync(self):
        self.__sync_datacenter()
        self.__sync_host()
        self.__sync_virtual_machine()

    def __sync_datacenter(self):
        # 同步数据中心数据
        datacenters = self.vcenter.get_datacenter()
        for datacenter in datacenters:
            exist = self.pgsql.execute(
                f'SELECT * FROM "vCenter_datacenter" WHERE vc_name=\'{self.vcenter.name}\' AND datacenter_id=\'{datacenter["datacenter"]}\'').fetchall()
            if not exist:
                self.pgsql.execute(
                    f'INSERT INTO "vCenter_datacenter" VALUES (\'{self.vcenter.name}\',\'{datacenter["name"]}\',\'{datacenter["datacenter"]}\')')

        # 删除vCenter已经不存在的数据
        old_data = self.pgsql.execute(
            f'SELECT * FROM "vCenter_datacenter" WHERE vc_name=\'{self.vcenter.name}\'').fetchall()
        for data in old_data:
            old_data_dict = {"name": data[1], "datacenter": data[2]}
            if old_data_dict not in datacenters:
                self.pgsql.execute(
                    f'DELETE FROM "vCenter_datacenter" WHERE vc_name=\'{self.vcenter.name}\' AND datacenter_id=\'{data[2]}\'')
                self.pgsql.execute(
                    f'INSERT INTO "vCenter_datacenter_archive" VALUES (\'{self.vcenter.name}\',\'{data[1]}\',\'{data[2]}\')')

    def __sync_host(self):
        # 同步宿主机数据
        datacenter_tup = self.pgsql.execute(
            f'SELECT datacenter_name, datacenter_id FROM "vCenter_datacenter" WHERE vc_name=\'{self.vcenter.name}\'').fetchall()
        for datacenter in datacenter_tup:
            hosts = self.vcenter.get_host(datacenter[1])
            for host in hosts:
                exist = self.pgsql.execute(
                    f'SELECT * FROM "vCenter_host" WHERE vc_name=\'{self.vcenter.name}\' AND host_id=\'{host["host"]}\' AND host_uuid=\'{host["uuid"]}\' AND host_name=\'{host["name"]}\' AND host_connection_state=\'{host["connection_state"]}\' AND host_power_state=\'{host["power_state"]}\' AND datacenter_name=\'{datacenter[0]}\'').fetchall()
                if not exist:
                    self.pgsql.execute(
                        f'INSERT INTO "vCenter_host" VALUES (\'{self.vcenter.name}\',\'{host["host"]}\', \'{host["uuid"]}\', \'{host["name"]}\',\'{host["connection_state"]}\',\'{host["power_state"]}\',\'{datacenter[0]}\')')

                # 删除vCenter已经不存在的数据
                old_data = self.pgsql.execute(
                    f'SELECT * FROM "vCenter_host" WHERE vc_name=\'{self.vcenter.name}\' AND datacenter_name=\'{datacenter[0]}\'').fetchall()
                for data in old_data:
                    old_data_dict = {"host": data[1], "name": data[3], "connection_state": data[4],
                                     "power_state": data[5],
                                     "uuid": data[2]}
                    if old_data_dict not in hosts:
                        self.pgsql.execute(
                            f'DELETE FROM "vCenter_host" WHERE vc_name=\'{self.vcenter.name}\' AND host_id=\'{data[1]}\'')
                        self.pgsql.execute(
                            f'INSERT INTO "vCenter_host_archive" VALUES (\'{self.vcenter.name}\',\'{data[1]}\',\'{data[2]}\',\'{data[3]}\',\'{data[4]}\', \'{data[5]}\', \'{datacenter[0]}\')')

        # 删除数据中心已不存在的宿主机数据
        old_host_datacenter = set(
            self.pgsql.execute(
                f'SELECT datacenter_name FROM "vCenter_host" WHERE vc_name=\'{self.vcenter.name}\'').fetchall())
        for host_datacenter in old_host_datacenter:
            # print(host_datacenter[0], [i[0] for i in datacenter_tup])
            if host_datacenter[0] not in [i[0] for i in datacenter_tup]:
                self.pgsql.execute(f'DELETE FROM "vCenter_host" WHERE datacenter_name=\'{host_datacenter[0]}\'')

    def __sync_virtual_machine(self):
        # 同步虚拟机数据
        host_tup = self.pgsql.execute(
            f'SELECT host_id, host_name, host_connection_state FROM "vCenter_host" WHERE vc_name=\'{self.vcenter.name}\' AND host_connection_state=\'CONNECTED\'').fetchall()
        for host in host_tup:
            vms = self.vcenter.get_vm(host[0])
            # if not host[2] == "DISCONNECTED":  # 已与vCenter断开链接的主机不再添加虚拟机数据
            for vm in vms:
                # ipaddress = self.vcenter.get_vm_info(vm["vm"])
                # ipaddress = None
                exist = self.pgsql.execute(
                    f'SELECT * FROM "vCenter_vm" WHERE vc_name=\'{self.vcenter.name}\' AND vm_id=\'{vm["vm"]}\' AND vm_uuid=\'{vm["uuid"]}\' AND vm_name=\'{vm["name"]}\' AND vm_ipaddress=\'{vm["ipaddress"]}\' AND vm_power_state=\'{vm["power_state"]}\' AND vm_cpu_count={vm["cpu_count"]} AND "vm_memory_size_MiB"={vm["memory_size_MiB"]} AND host_name=\'{host[1]}\'').fetchall()
                # print(exist)
                if not exist:
                    self.pgsql.execute(
                        f'INSERT INTO "vCenter_vm" VALUES (\'{self.vcenter.name}\',\'{vm["vm"]}\',\'{vm["uuid"]}\',\'{vm["name"]}\',\'{vm["ipaddress"]}\',\'{vm["power_state"]}\',{vm["cpu_count"]},{vm["memory_size_MiB"]},\'{host[1]}\')')

            # 删除vCenter已经不存在的数据
            old_data = self.pgsql.execute(
                f'SELECT * FROM "vCenter_vm" WHERE vc_name=\'{self.vcenter.name}\' AND host_name=\'{host[1]}\'').fetchall()
            for data in old_data:
                old_data_dict = {"memory_size_MiB": data[7], "vm": data[1], "name": data[3], "power_state": data[5],
                                 "cpu_count": data[6], "ipaddress": data[4], "uuid": data[2]}
                if old_data_dict not in vms:
                    self.pgsql.execute(
                        f'DELETE FROM "vCenter_vm" WHERE vc_name=\'{self.vcenter.name}\' AND vm_id=\'{data[1]}\'')
                    self.pgsql.execute(
                        f'INSERT INTO "vCenter_vm_archive" VALUES (\'{self.vcenter.name}\',\'{data[1]}\',\'{data[2]}\',\'{data[3]}\',\'{data[4]}\',\'{data[5]}\',{data[6]},{data[7]},\'{host[1]}\')')

        # 删除宿主机已不存在的虚拟机数据
        old_vm = set(
            self.pgsql.execute(f'SELECT host_name FROM "vCenter_vm" WHERE vc_name=\'{self.vcenter.name}\'').fetchall())
        for vm_host in old_vm:
            if vm_host[0] not in [i[1] for i in host_tup]:
                self.pgsql.execute(f'DELETE FROM "vCenter_vm" WHERE host_name=\'{vm_host[0]}\'')

    def __del__(self):
        self.conn.commit()
        self.pgsql.close()
        self.conn.close()


def start_process(certificate):
    vcenter_obj = Vcenter(certificate[1], certificate[2], certificate[3], certificate[0])
    # print(vcenter_obj)
    run = DataProcess(vcenter_obj)
    run.start_sync()


def main():
    logger.info('开始同步外网的vCenter数据到PostgreSQL。')
    start_time = time.time()

    pgsql = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    certificate_tup = pgsql.execute('SELECT * FROM "vCenter_certficate" WHERE internal = false').fetchall()

    processes = []
    for certificate in certificate_tup:
        process = multiprocessing.Process(target=start_process, args=(certificate,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    end_time = time.time()
    process_time = end_time - start_time
    logger.info("外网vCenter数据同步到PostgreSQL执行完成。耗时：%.1f 秒" % process_time)


if __name__ == '__main__':
    main()
