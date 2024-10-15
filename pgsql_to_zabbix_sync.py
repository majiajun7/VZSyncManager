import logging
import re
import time

import psycopg
from pyzabbix import ZabbixAPI

from zabbix_tools import Zabbix
from log_handler import get_logger

'''
1.执行此脚本之前需执行同步vcenter数据库脚本
2.获取zabbix中的主机清单，与vcenter宿主机数据进行比对。若zabbix中不存在该主机，则创建主机群组与主机(主机加入到对应区域群组中)。如已存在，检测主机宏中UUID、URL与所属区域群组信息是否为最新数据。不是则自动更新。
3.获取zabbix中的主机清单，与vcenter虚拟机数据进行比对。若zabbix中不存在该主机，则创建主机，并加入到所在宿主机群组中。如已存在，检测主机宏中UUID、URL与所属宿主机群组信息是否为最新数据。不是则自动更新。
3.自动发现的新宿主机默认加入到“Discovered hosts”主机群组内
4.“IT中心云桌面VCenter”和“IT中心测试区VCenter”仅自动创建宿主机，不会自动创建虚拟机。

'''

logger = get_logger(__name__)


def get_host_group_id(host_name, zabbix):
    """
    根据宿主机IP地址获取宿主机在zabbix中的主机群组ID，如不存在则创建新的。
    :param host_name:
    :param zabbix:
    :return: 宿主机在zabbix中的主机群组ID
    """
    # 通过数据库中的host_name字段查找zabbix内与其对应的主机群组ID
    group_info = zabbix.get_host_group_all()
    for group in group_info["result"]:
        # if host[3] in group["name"]:
        if re.search("%s$" % host_name, group["name"]):
            group_id = group["groupid"]
            print("%s 该宿主机已存在主机群组，跳过主机群组创建步骤" % host_name)
            return group_id
    else:
        group_id = zabbix.create_host_group(host_name)["result"]["groupids"][0]
        logger.info('宿主机主机群组 %s 创建成功 群组ID:%s' % (host_name, group_id))
        return group_id


def get_macro(macros):
    """
    根据传入的源数据获取{$VMWARE.URL}和{$VMWARE.HV.UUID}的值，并返回
    :param macros:
    :param zabbix:
    :return: 返回元组({$VMWARE.URL}, {$VMWARE.HV.UUID})
    """

    # 遍历zabbix主机宏，查找zabbix宏的值是否等于数据库中宏的值，并使用result变量标记查找结果。
    result_url, result_uuid = "", ""

    for macro in macros:
        if macro["macro"] == "{$VMWARE.URL}":
            result_url = macro["value"]
        elif macro["macro"] == "{$VMWARE.HV.UUID}" or macro["macro"] == "{$VMWARE.VM.UUID}":
            result_uuid = macro["value"]

        # 如果找到了两个宏，提前结束循环
        if result_url and result_uuid:
            break
    return (result_url, result_uuid)


def run():
    zapi = ZabbixAPI("http://10.20.120.239")
    zapi.login("jiajun.ma", "Abc000425.")

    conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    pgsql = conn.cursor()

    zabbix_obj = Zabbix()

    hosts = pgsql.execute('SELECT * FROM "vCenter_host" WHERE host_connection_state=\'CONNECTED\'').fetchall()
    area_gid_dict = {"IT中心管理VCenter": "238",
                     "IT中心云桌面VCenter": "288",
                     "IT中心DMZ区VCenter": "330",
                     "IT中心产线区VCenter": "329",
                     "IT中心研发域VCenter": "567",
                     "IT中心测试区VCenter": "672",
                     "IT中心物理内网VCenter": "602",
                     "IT中心物理内网云桌面VCenter": "873"}

    for host in hosts:
        # 若主机已存在：
        # 1. 将主机名称同步到主机群组名称。
        # 2. 检测主机宏中UUID与URL与主机群组是否为最新数据。不是则更新。
        # 3. 检测所属区域群组信息是否为最新数据，不是则更新。
        # logging.info('正在处理宿主机：%s', host[3])

        # zabbix_host = zabbix_obj.check_host_exist(host[2], host[3])
        zabbix_host = zabbix_obj.check_host_exist(host[2], host[3])

        # 主机在zabbix中存在则检查更新zabbix主机数据
        if zabbix_host:
            group_id = get_host_group_id(host[3], zabbix_obj)

            # 数据库host_name字段与zabbix主机的name不匹配则重命名zabbix主机，适用于vcenter宿主机IP互换情况。
            # if re.search(".*%s.*" % host[3], zabbix_host["name"]) is None:
            if not host[3] in zabbix_host["name"]:
                # 获取原来的接口信息
                interfaces = zabbix_host["interfaces"].copy()
                # 修改接口信息1中的IP为当前宿主机IP
                interfaces[0]["ip"] = host[3]

                zabbix_obj.update_host(zabbix_host["hostid"], name=host[2], displayname=host[3], interface=interfaces)
                zabbix_host['name'] = host[3]
                print(zabbix_host["hostid"], host[2], host[3], interfaces)
                logger.info(
                    '检测到zabbix主机名称与UUID不匹配，将主机名称 %s 重命名为 %s' % (zabbix_host["name"], host[3]))

            # 对比主机群组和zabbix主机名称是否一致，不一致则将主机群组名称改为与主机名称相同
            # group_info = zabbix_obj.get_host_group_all()
            # print(zapi.hostgroup.get(groupids=group_id))
            group_info = zapi.hostgroup.get(groupids=group_id)[0]
            # ipaddress = zabbix_host["interfaces"][0]["ip"]
            # ipaddress = host[3]
            # for group in group_info["result"]:
            # if re.search(".*%s$" % ipaddress, group["name"]):
            # 如果zabbix主机群组包含vcenter宿主机IP，则不进行重命名，跳出该步骤
            # 如何zabbix主机群组名称和zabbix主机可见名称不一致，则重命名zabbix主机群组名称
            if group_info["name"] != zabbix_host['name']:
                zabbix_obj.update_host_group(group_id, zabbix_host['name'])
                logger.info('将主机群组 %s 重命名为 %s' % (group_info['name'], zabbix_host['name']))

            host_url, host_uuid = get_macro(zabbix_host["macros"])

            # 将zabbix主机信息宏UUID和URL与数据库中做对比，判断是否已有改变。
            # 通过宿主机uuid查询宿主机所属vc名称
            # host_name = pgsql.execute('SELECT vc_name FROM "vCenter_vm" WHERE vm_uuid=\'%s\'' % host[2]).fetchall()[0][0]
            # 通过vc名称查询vcurl
            # vc_url = \
            # pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE host_name=\'%s\'' % host_name).fetchall()[0][0]
            host_vc_url = \
            pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                0] + "/sdk"
            if host_url != host_vc_url or host_uuid != host[2]:
                host_macro = [{
                    "macro": "{$VMWARE.URL}",
                    "value": host_vc_url
                },
                    {
                        "macro": "{$VMWARE.HV.UUID}",
                        "value": host[2],
                    }]

                # 此处针对宿主机的所在vCenter区域群组做修改。先删除旧的主机群组ID，再添加新的主机群组ID，再进行UPDATE。
                host_group_id = []
                for group in zabbix_host["groups"]:
                    host_group_id.append({"groupid": group["groupid"]})
                for k, v in area_gid_dict.items():
                    if host_group_id.count({"groupid": v}):
                        host_group_id.remove({"groupid": v})
                host_group_id.append({"groupid": area_gid_dict[host[0]]})
                zabbix_obj.update_host(zabbix_host["hostid"], zabbix_host["name"], host_macro, host_group_id,
                                       name=host[2])
                logger.info('检测到zabbix宿主机与vCenter不一致，更新宿主机信息，已完成。')
        # 若不存在该主机，则创建主机群组与主机
        else:
            interface = [
                {
                    "type": 1,
                    "main": 1,
                    "useip": 1,
                    "ip": host[3],
                    "dns": "",
                    "port": "443"
                },
                {
                    "type": 2,
                    "main": 1,
                    "useip": 1,
                    "ip": host[3],
                    "dns": "",
                    "port": "161",
                    "details": {
                        "version": 2,
                        "bulk": 1,
                        "community": "{$SNMP_COMMUNITY}",
                    }
                }
            ]
            host_vc_url = \
            pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                0] + "/sdk"
            host_macro = [{
                "macro": "{$VMWARE.URL}",
                "value": host_vc_url,
            },
                {
                    "macro": "{$VMWARE.HV.UUID}",
                    "value": host[2]
                }
            ]

            zabbix_obj.create_host(host[2], host[3], area_gid_dict[host[0]], 10123, interface, host_macro)
            logger.info('%s 宿主机在zabbix中不存在，自动创建' % host[3])

        # 开始处理宿主机下的虚拟机数据
        # blacklist_ips = {'10.50.68.18', '10.50.68.19', '10.50.68.20', '10.50.68.21', '10.50.68.22', '10.50.68.11',
        #                '10.50.68.12', '10.50.68.13', '10.50.68.14', '10.50.68.15', '10.50.68.16', '10.50.68.17'}
        # if host[0] != "IT中心云桌面VCenter" and host[3] not in blacklist_ips:
        if host[0] != "IT中心云桌面VCenter" and host[0] != "IT中心测试区VCenter" and host[0] != "IT中心物理内网云桌面VCenter":
            vms = pgsql.execute(
                'SELECT * FROM "vCenter_vm" WHERE host_name=\'%s\' AND vc_name != \'IT中心云桌面VCenter\'' % host[
                    3]).fetchall()
            for vm in vms:
                # logging.info('正在处理虚拟机：%s', vm[3])
                # zabbix_vm_host = zabbix_obj.check_vm_host_exist(vm[2])
                zabbix_vm_host = zabbix_obj.check_vm_host_exist(vm[2])
                # 检测主机宏中UUID、URL与所属宿主机群组信息是否为最新数据，不是则更新
                if zabbix_vm_host:
                    # for i in zabbix_vm_host["macros"]:
                    #     if i["macro"] == "{$VMWARE.VM.UUID}":
                    #         result = i["value"]
                    #         break
                    # else:
                    #     result = ""
                    #
                    # for i in zabbix_vm_host["macros"]:
                    #     if i["macro"] == "{$VMWARE.URL}":
                    #         result1 = i["value"]
                    #         break
                    # else:
                    #     result1 = ""
                    #
                    vm_url, vm_uuid = get_macro(zabbix_vm_host["macros"])

                    # 当zabbix主机可见名称与vcenter不一致或主机uuid与url值与vcenter数据不符时，则将vm_host_macro变量赋值，在后面对主机进行update
                    # 通过vc名称查询vcurl
                    vm_vc_url = \
                    pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vm[0]).fetchall()[0][
                        0] + "/sdk"
                    if zabbix_vm_host["name"] != vm[3] or vm_url != vm_vc_url or vm_uuid != vm[2]:
                        # print(zabbix_vm_host["name"] != vm[3] , vm_url != vm_vc_url , vm_uuid != vm[2])
                        # if not (zabbix_vm_host["name"] == vm[3] and result == vm[2] and result1 == vc_url + "/sdk"):
                        #     logging.info('%s 该zabbix虚拟机主机信息与vcenter虚拟机信息不一致，更新数据' % vm[3])
                        vm_host_macro = [{
                            "macro": "{$VMWARE.URL}",
                            "value": vm_vc_url
                        },
                            {
                                "macro": "{$VMWARE.VM.UUID}",
                                "value": vm[2],
                            }]

                    vm_group_info = zabbix_vm_host['groups']
                    # print(vm[3], vm_group_info)
                    # 判断宿主机IP是否在zabbix虚拟机所属群组名称内，存在则跳过，不存在则先删除组，再更新最新所属组数据。
                    for group in vm_group_info:
                        if host[3] in group['name']:
                            break
                    else:
                        # 如果主机群组名称中包含宿主机IP，则记录下来，先删除该组，再更新最新所属组数据。
                        for group in vm_group_info:
                            # 寻找要删除的组，组的名称会包含IP地址
                            if '10.' in group['name'] or '192.' in group['name']:
                                remove_group_id = group['groupid']
                                break

                        # if 'remove_group_id' in locals():
                        vm_host_group = []
                        for group in zabbix_vm_host["groups"]:
                            vm_host_group.append({"groupid": group["groupid"]})
                        if 'remove_group_id' in locals():
                            vm_host_group.remove({"groupid": remove_group_id})
                            # 删除变量，防止下一次循环判断使用旧的值
                            del remove_group_id
                        vm_host_group.append({"groupid": group_id})

                    # 如果宏有变动则值存在更新主机包含宏信息，如宏未变动则无需更新主机宏信息
                    if 'vm_host_macro' in locals() and 'vm_host_group' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], vm_host_macro, vm_host_group)
                        logger.info('更新 %s 虚拟机主机宏和所属组完毕' % vm[3])
                        # 删除变量，防止下一次循环判断使用旧的值
                        del vm_host_macro
                        del vm_host_group
                    elif 'vm_host_macro' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], vm_host_macro)
                        logger.info('更新 %s 虚拟机主机宏完毕' % vm[3])
                        del vm_host_macro
                    elif 'vm_host_group' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], group_list=vm_host_group)
                        logger.info('更新 %s 虚拟机所属组完毕' % vm[3])
                        del vm_host_group
                    # else:
                    #     zabbix_obj.update_host(vm_host["hostid"], vm[3])

                # 若zabbix中不存在该虚拟机主机，则创建虚拟机主机，并加入到所在宿主机群组中。
                else:
                    interface = [
                        {
                            "type": 1,
                            "main": 1,
                            "useip": 1,
                            "ip": vm[4],
                            "dns": "",
                            "port": "10050"
                        }
                    ]
                    vm_vc_url = \
                    pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vm[0]).fetchall()[0][
                        0] + "/sdk"
                    vm_host_macro = [{
                        "macro": "{$VMWARE.URL}",
                        "value": vm_vc_url,
                    },
                        {
                            "macro": "{$VMWARE.VM.UUID}",
                            "value": vm[2]
                        }
                    ]
                    zabbix_obj.create_host(vm[2], vm[3], group_id, 10124, interface, vm_host_macro)
                    logger.info('虚拟机 %s 主机创建成功' % vm[3])
                    # 删除变量，防止下一次循环被调用
                    del vm_host_macro

    pgsql.close()
    conn.close()


def main():
    logger.info('开始执行zabbix数据同步脚本。')
    start_time = time.time()

    run()

    end_time = time.time()
    process_time = end_time - start_time
    logger.info('zabbix数据同步脚本执行完成。耗时：%d 秒', process_time)


if __name__ == '__main__':
    main()
