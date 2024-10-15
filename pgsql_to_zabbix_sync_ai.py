import logging
import re
import time
import psycopg
from pyzabbix import ZabbixAPI
from zabbix_tools import Zabbix
from log_handler import get_logger

logger = get_logger(__name__)

def get_host_group_id(host_name, zabbix):
    group_info = zabbix.get_host_group_all()
    for group in group_info["result"]:
        if re.search(rf"{host_name}$", group["name"]):
            group_id = group["groupid"]
            logger.info(f"{host_name} 该宿主机已存在主机群组，跳过主机群组创建步骤")
            return group_id
    else:
        group_id = zabbix.create_host_group(host_name)["result"]["groupids"][0]
        logger.info(f'宿主机主机群组 {host_name} 创建成功 群组ID:{group_id}')
        return group_id

def get_macro(macros):
    result_url, result_uuid = "", ""
    for macro in macros:
        if macro["macro"] == "{$VMWARE.URL}":
            result_url = macro["value"]
        elif macro["macro"] in {"{$VMWARE.HV.UUID}", "{$VMWARE.VM.UUID}"}:
            result_uuid = macro["value"]
        if result_url and result_uuid:
            break
    return (result_url, result_uuid)

def run():
    zapi = ZabbixAPI("http://10.20.120.239")
    zapi.login("jiajun.ma", "Abc000425.")
    conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    pgsql = conn.cursor()
    zabbix_obj = Zabbix()

    pgsql.execute('SELECT * FROM "vCenter_host" WHERE host_connection_state=\'CONNECTED\'')
    hosts = pgsql.fetchall()

    area_gid_dict = {
        "IT中心管理VCenter": "238",
        "IT中心云桌面VCenter": "288",
        "IT中心DMZ区VCenter": "330",
        "IT中心产线区VCenter": "329",
        "IT中心研发域VCenter": "567",
        "IT中心测试区VCenter": "672"
    }

    for host in hosts:
        zabbix_host = zabbix_obj.check_host_exist(host[2], host[3])
        if zabbix_host:
            group_id = get_host_group_id(host[3], zabbix_obj)
            if not host[3] in zabbix_host["name"]:
                interfaces = zabbix_host["interfaces"].copy()
                interfaces[0]["ip"] = host[3]
                zabbix_obj.update_host(zabbix_host["hostid"], name=host[2], displayname=host[3], interfaces=interfaces)
                logger.info(f'检测到zabbix主机可见名称与vCenter宿主机名称不一致，将zabbix主机可见名称 {zabbix_host["name"]} 重命名为 {host[3]}')
                zabbix_host['name'] = host[3]  # 更新本地变量以反映更改

            group_info = zapi.hostgroup.get(groupids=group_id)[0]
            if group_info["name"] != zabbix_host['name']:
                zabbix_obj.update_host_group(group_id, zabbix_host['name'])
                logger.info(f'将主机群组 {group_info["name"]} 重命名为 {zabbix_host["name"]}')

            host_url, host_uuid = get_macro(zabbix_host["macros"])
            pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=%s', (host[0],))
            host_vc_url = pgsql.fetchone()[0] + "/sdk"
            if host_url != host_vc_url or host_uuid != host[2]:
                host_macro = [{"macro": "{$VMWARE.URL}", "value": host_vc_url},
                              {"macro": "{$VMWARE.HV.UUID}", "value": host[2]}]
                # 获取当前主机所属的所有群组
                current_groups = [{"groupid": group["groupid"]} for group in zabbix_host["groups"]]

                # 检查并移除旧的区域群组
                for area, area_group_id in area_gid_dict.items():
                    if {"groupid": area_group_id} in current_groups and area != host[0]:
                        current_groups.remove({"groupid": area_group_id})

                # 添加新的区域群组信息
                new_group_id = area_gid_dict[host[0]]
                if {"groupid": new_group_id} not in current_groups:
                    current_groups.append({"groupid": new_group_id})

                # 使用更新后的群组列表和宏更新主机信息
                zabbix_obj.update_host(zabbix_host["hostid"], host[2], host_macro, current_groups)
                logger.info('检测到zabbix宿主机与vCenter不一致，更新宿主机信息，已完成。')
        else:
            interface = [{"type": 1, "main": 1, "useip": 1, "ip": host[3], "dns": "", "port": "443"},
                         {"type": 2, "main": 1, "useip": 1, "ip": host[3], "dns": "", "port": "161",
                          "details": {"version": 2, "bulk": 1, "community": "{$SNMP_COMMUNITY}"}}]
            host_vc_url = pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=%s', (host[0],)).fetchone()[0] + "/sdk"
            host_macro = [{"macro": "{$VMWARE.URL}", "value": host_vc_url},
                          {"macro": "{$VMWARE.HV.UUID}", "value": host[2]}]
            zabbix_obj.create_host(host[2], host[3], area_gid_dict[host[0]], 10123, interface, host_macro)
            logger.info(f'{host[3]} 宿主机在zabbix中不存在，自动创建')

        if host[0] != "IT中心云桌面VCenter" and host[0] != "IT中心测试区VCenter":
            # pgsql.execute('SELECT * FROM "vCenter_vm" WHERE host_name=%s AND vc_name != %s', (host[3], "IT中心云桌面VCenter"))
            pgsql.execute('SELECT * FROM "vCenter_vm" WHERE host_name=%s', (host[3],))
            vms = pgsql.fetchall()
            for vm in vms:
                zabbix_vm_host = zabbix_obj.check_vm_host_exist(vm[2])
                if zabbix_vm_host:
                    vm_url, vm_uuid = get_macro(zabbix_vm_host["macros"])
                    pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=%s', (vm[0],))
                    vm_vc_url = pgsql.fetchone()[0] + "/sdk"
                    if zabbix_vm_host["name"] != vm[3] or vm_url != vm_vc_url or vm_uuid != vm[2]:
                        # print(zabbix_vm_host["name"] != vm[3] ,vm_url != vm_vc_url ,vm_uuid != vm[2])
                        # print(vm_uuid, vm[2])
                        vm_host_macro = [{"macro": "{$VMWARE.URL}", "value": vm_vc_url},
                                         {"macro": "{$VMWARE.VM.UUID}", "value": vm[2]}]

                        # 获取虚拟机当前所属的所有群组
                        vm_group_info = zabbix_vm_host["groups"]

                        # 初始化要删除的群组ID列表
                        remove_group_ids = []

                        # 遍历虚拟机当前所属的群组
                        for group in vm_group_info:
                            # 检查群组名称是否包含特定的IP地址段
                            if '10.' in group['name'] or '192.' in group['name']:
                                remove_group_ids.append(group['groupid'])

                        # 移除包含特定IP地址段的群组
                        current_vm_groups = [{"groupid": group["groupid"]} for group in zabbix_vm_host["groups"]]
                        current_vm_groups = [group for group in current_vm_groups if
                                             group["groupid"] not in remove_group_ids]

                        # 添加虚拟机到正确的宿主机群组
                        correct_host_group_id = get_host_group_id(host[3], zabbix_obj)
                        if {"groupid": correct_host_group_id} not in current_vm_groups:
                            current_vm_groups.append({"groupid": correct_host_group_id})

                        # 使用更新后的群组信息更新虚拟机
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], vm_host_macro, current_vm_groups)
                        logger.info(f'更新 {vm[3]} 虚拟机所属组信息')

                else:
                    interface = [{"type": 1, "main": 1, "useip": 1, "ip": vm[4], "dns": "", "port": "10050"}]
                    vm_vc_url = pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=%s', (vm[0],)).fetchone()[0] + "/sdk"
                    vm_host_macro = [{"macro": "{$VMWARE.URL}", "value": vm_vc_url},
                                     {"macro": "{$VMWARE.VM.UUID}", "value": vm[2]}]
                    host_group_id = get_host_group_id(host[3], zabbix_obj)
                    zabbix_obj.create_host(vm[2], vm[3], host_group_id, 10124, interface, vm_host_macro)
                    logger.info(f'虚拟机 {vm[3]} 主机创建成功')

    pgsql.close()
    conn.close()

def main():
    logger.info('开始执行zabbix数据同步脚本。')
    start_time = time.time()
    run()
    end_time = time.time()
    process_time = end_time - start_time
    logger.info(f'zabbix数据同步脚本执行完成。耗时：{int(process_time)} 秒')

if __name__ == '__main__':
    main()
