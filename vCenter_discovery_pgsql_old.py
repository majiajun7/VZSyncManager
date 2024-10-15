import re
import psycopg
import time
from zabbix import Zabbix


'''
1.执行此脚本之前需执行同步vcenter数据库脚本
2.获取zabbix中的主机清单，与vcenter宿主机数据进行比对。若zabbix中不存在该主机，则创建主机群组与主机(主机加入到对应区域群组中)。如已存在，检测主机宏中UUID、URL与所属区域群组信息是否为最新数据。不是则自动更新。
3.获取zabbix中的主机清单，与vcenter虚拟机数据进行比对。若zabbix中不存在该主机，则创建主机，并加入到所在宿主机群组中。如已存在，检测主机宏中UUID与URL是否为最新数据。不是则自动更新。
3.自动发现的新宿主机默认加入到“Discovered hosts”主机群组内
4.“IT中心云桌面VCenter”仅自动创建宿主机，不会自动创建虚拟机。

'''
def main():
    conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    pgsql = conn.cursor()

    zabbix_obj = Zabbix()
    hosts = pgsql.execute('SELECT * FROM "vCenter_host" WHERE host_connection_state=\'CONNECTED\'').fetchall()
    area_gid_dict = {"IT中心管理VCenter": "238",
                "IT中心云桌面VCenter": "288",
                "IT中心DMZ区VCenter": "330",
                "IT中心产线区VCenter": "329",
                     "IT中心研发域VCenter": "567",
                     "IT中心测试区VCenter": "672"}

    for host in hosts:
        # 通过数据库中的host_name字段查找zabbix内与其对应的主机群组ID
        group_info = zabbix_obj.get_host_group_all()
        for group in group_info["result"]:
            # if host[3] in group["name"]:
            if re.search("%s$" % host[3], group["name"]):
                group_id = group["groupid"]
                # print("%s 该宿主机已存在主机群组，跳过主机群组创建步骤" % host[3])
                break
        else:
            group_id = zabbix_obj.create_host_group(host[3])["result"]["groupids"][0]
            print('%s 宿主机主机群组创建成功 群组ID:%s' % (host[3], group_id))
        # 若主机已存在：
        # 1. 将主机名称同步到主机群组名称。
        # 2. 检测主机宏中UUID与URL与主机群组是否为最新数据。不是则更新。
        # 3. 检测所属区域群组信息是否为最新数据，不是则更新。
        zabbix_host = zabbix_obj.check_host_exist(host[2], host[3])
        if zabbix_host:
            # 数据库host_name字段不存在于zabbix主机的name中则重命名zabbix主机，适用于vcenter宿主机IP互换情况。

            if re.search(".*%s.*" % host[3], zabbix_host["name"]) is None:
                # 获取原来的接口信息
                interfaces = zabbix_host["interfaces"].copy()
                # 修改接口信息1中的IP为当前宿主机IP
                interfaces[0]["ip"] = host[3]

                zabbix_obj.update_host(zabbix_host["hostid"], name=host[2], displayname=host[3], interface=interfaces)
                print('检测到zabbix主机名称与UUID不匹配，将主机名称 %s 重命名为 %s' % (zabbix_host["name"], host[3]))
                # print(zabbix_host["hostid"], host[2], host[3])

            # 对比主机群组和主机名称是否一致，不一致则将主机群组名称改为与主机名称相同
            group_info = zabbix_obj.get_host_group_all()
            ipaddress = zabbix_host["interfaces"][0]["ip"]
            for group in group_info["result"]:
                if re.search(".*%s$" % ipaddress, group["name"]):
                    # 如果找到有zabbix主机群组等于vcenter宿主机的名称，则不进行重命名，跳出该步骤
                    if zabbix_host['name'] == group["name"]:
                        break
                    group_id = group["groupid"]
                    zabbix_obj.update_host_group(group_id, zabbix_host["name"])
                    print('将主机群组 %s 重命名为 %s' % (group['name'], zabbix_host['name']))
                    break
            else:
                print("error：主机群组重命名失败，找不到宿主机对应的主机群组名称")
                exit()

            for i in zabbix_host["macros"]:
                if i["macro"] == "{$VMWARE.HV.UUID}":
                    result = i["value"]
                    break
            else:
                result = ""

            for i in zabbix_host["macros"]:
                if i["macro"] == "{$VMWARE.URL}":
                    result1 = i["value"]
                    break
            else:
                result1 = ""

            # 与最新宏UUID和URL数据做对比，判断是否已有改变
            # 通过宿主机uuid查询宿主机所属vc名称
            # host_name = pgsql.execute('SELECT vc_name FROM "vCenter_vm" WHERE vm_uuid=\'%s\'' % host[2]).fetchall()[0][0]
            # 通过vc名称查询vcurl
            # vc_url = \
            # pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE host_name=\'%s\'' % host_name).fetchall()[0][0]
            if not (result == host[2] and result1 == str(
                    pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                        0]) + "/sdk"):
                host_macro = [{
                    "macro": "{$VMWARE.URL}",
                    "value": str(
                        pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                            0]) + "/sdk"
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
                zabbix_obj.update_host(zabbix_host["hostid"], zabbix_host["name"], host_macro, host_group_id, name=host[2])

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
            host_macro = [{
                "macro": "{$VMWARE.URL}",
                "value": str(
                    pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                        0]) + "/sdk",
            },
                {
                    "macro": "{$VMWARE.HV.UUID}",
                    "value": host[2]
                }
            ]

            zabbix_obj.create_host(host[2], host[3], area_gid_dict[host[0]], 10123, interface, host_macro)
            print('%s 宿主机在zabbix中不存在，自动创建' % host[3])

        # 开始处理宿主机下的虚拟机数据
        blacklist_ips = {'10.50.68.18', '10.50.68.19', '10.50.68.20', '10.50.68.21', '10.50.68.22', '10.50.68.11',
                       '10.50.68.12', '10.50.68.13', '10.50.68.14', '10.50.68.15', '10.50.68.16', '10.50.68.17'}
        if host[0] != "IT中心云桌面VCenter" and host[3] not in blacklist_ips:
            vms = pgsql.execute(
                'SELECT * FROM "vCenter_vm" WHERE host_name=\'%s\' AND vc_name != \'IT中心云桌面VCenter\'' % host[3]).fetchall()
            for vm in vms:
                zabbix_vm_host = zabbix_obj.check_vm_host_exist(vm[2]) # 判断主机是否存在的条件没有精确性
                # 检测主机宏中UUID、URL与所属宿主机群组信息是否为最新数据，不是则更新
                if zabbix_vm_host:
                    for i in zabbix_vm_host["macros"]:
                        if i["macro"] == "{$VMWARE.VM.UUID}":
                            result = i["value"]
                            break
                    else:
                        result = ""

                    for i in zabbix_vm_host["macros"]:
                        if i["macro"] == "{$VMWARE.URL}":
                            result1 = i["value"]
                            break
                    else:
                        result1 = ""

                    # 当zabbix主机可见名称与vcenter不一致或主机uuid与url值与vcenter数据不符时，则将macro变量赋值，在后面对主机进行update
                    # 通过虚拟机uuid查询虚拟机所属vc名称，如果查到多个结果取最后一个，一般是由于宿主机在多个vc被纳管。
                    vc_name = pgsql.execute('SELECT vc_name FROM "vCenter_vm" WHERE vm_uuid=\'%s\'' % vm[2]).fetchall()[0][-1]
                    # 通过vc名称查询vcurl
                    vc_url = pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vc_name).fetchall()[0][0]
                    if not (zabbix_vm_host["name"] == vm[3] and result == vm[2] and result1 == vc_url + "/sdk"):
                        print('%s 该zabbix虚拟机主机信息与vcenter虚拟机信息不一致，更新数据' % vm[3])
                        vm_host_macro = [{
                            "macro": "{$VMWARE.URL}",
                            "value": vc_url + "/sdk"
                        },
                            {
                                "macro": "{$VMWARE.VM.UUID}",
                                "value": vm[2],
                            }]


                    host_ip = [ip[0] for ip in pgsql.execute('SELECT host_name FROM "vCenter_host"').fetchall()]
                    host_group_info = zabbix_vm_host['groups']
                    # 判断主机所属宿主机群组是否正确
                    for group in host_group_info:
                        if host[3] in group['name']:
                            break
                    else:
                        # 如果主机群组名称中包含宿主机IP，则记录下来，先删除该组，再更新最新所属组数据。
                        for group in host_group_info:
                            if '10.' in group['name'] or '192.' in group['name']:
                                        remove_group_id = group['groupid']
                                        break

                        if 'remove_group_id' in locals():
                            vm_host_group = []
                            for group in zabbix_vm_host["groups"]:
                                vm_host_group.append({"groupid": group["groupid"]})
                            vm_host_group.remove({"groupid": remove_group_id})
                            vm_host_group.append({"groupid": group_id})
                            # 删除变量，防止下一次循环判断使用旧的值
                            del remove_group_id


                    # 如果宏有变动则值存在更新主机包含宏信息，如宏未变动则无需更新主机宏信息
                    if 'vm_host_macro' in locals() and 'vm_host_group' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], vm_host_macro, vm_host_group)
                        # 删除变量，防止下一次循环判断使用旧的值
                        del vm_host_macro
                        del vm_host_group
                    elif 'vm_host_macro' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], vm_host_macro)
                        del vm_host_macro
                    elif 'vm_host_group' in locals():
                        zabbix_obj.update_host(zabbix_vm_host["hostid"], vm[3], group_list=vm_host_group)
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
                    vm_host_macro = [{
                        "macro": "{$VMWARE.URL}",
                        "value": str(
                            pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vm[0]).fetchall()[0][
                                0]) + "/sdk",
                    },
                        {
                            "macro": "{$VMWARE.VM.UUID}",
                            "value": vm[2]
                        }
                    ]
                    zabbix_obj.create_host(vm[2], vm[3], group_id, 10124, interface, vm_host_macro)
                    print('%s 虚拟机主机创建成功' % vm[3])
                    # 删除变量，防止下一次循环被调用
                    del vm_host_macro

    pgsql.close()
    conn.close()



if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    process_time = end_time - start_time
    print("zabbix数据同步完毕，本次处理耗时 %d 秒" % process_time)
