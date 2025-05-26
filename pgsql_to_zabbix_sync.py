import re
import time
import psycopg
import pyzabbix
from pyzabbix import ZabbixAPI
from zabbix_tools import Zabbix
from log_handler import get_logger

logger = get_logger(__name__)


def get_host_group_id(host_name, zabbix):
    """
    根据宿主机IP地址获取宿主机在zabbix中的主机群组ID，如不存在则创建新的。
    :param host_name:
    :param zabbix:
    :return: 宿主机在zabbix中的主机群组ID
    """
    group_info = zabbix.get_host_group_all()
    for group in group_info["result"]:
        if re.search("%s$" % host_name, group["name"]):
            group_id = group["groupid"]
            # print("%s 该宿主机已存在主机群组，跳过主机群组创建步骤" % host_name)
            return group_id
    else:
        group_id = zabbix.create_host_group(host_name)["result"]["groupids"][0]
        logger.info('宿主机主机群组 %s 创建成功 群组ID:%s' % (host_name, group_id))
        return group_id


def get_macro(macros):
    """
    获取宏的值，并返回VMWARE.URL和VMWARE.HV.UUID
    :param macros:
    :return: 返回元组({$VMWARE.URL}, {$VMWARE.HV.UUID})
    """
    result_url, result_uuid = "", ""
    for macro in macros:
        if macro["macro"] == "{$VMWARE.URL}":
            result_url = macro["value"]
        elif macro["macro"] == "{$VMWARE.HV.UUID}" or macro["macro"] == "{$VMWARE.VM.UUID}":
            result_uuid = macro["value"]
        if result_url and result_uuid:
            break
    return result_url, result_uuid


def cleanup_unused_host_groups(zabbix_obj, area_gid_dict):
    """
    清理与宿主机相关且无虚拟机的主机群组。
    :param zabbix_obj: Zabbix API对象
    """
    # 获取所有主机群组
    host_groups = zabbix_obj.get_host_group_all()["result"]

    # 正则表达式用于匹配IPv4地址
    ip_pattern = r'((25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.){3}(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'

    # 获取所有主机，并提取 IP 地址
    host_ips = set()
    for group_id in area_gid_dict.values():
        group_hosts = zabbix_obj.get_hosts_by_group(group_id)["result"]
        for host in group_hosts:
            match = re.search(ip_pattern, host['name'])
            if match:
                host_ips.add(match.group())

    # 遍历所有群组，检查是否需要清理
    for group in host_groups:
        group_id = group['groupid']
        group_name = group['name']

        # 从群组名称中提取IP地址
        match = re.search(ip_pattern, group_name)
        if match:
            # 提取匹配到的IP地址
            ip_address = match.group()

            if ip_address not in host_ips:
                # 如果没有该监控主机，检查Zabbix中该群组下的主机
                hosts_in_group = zabbix_obj.get_hosts_by_group(group_id)

                # 如果该群组下没有主机，删除该群组
                if not hosts_in_group["result"]:
                    zabbix_obj.delete_host_group(group_id)
                    logger.info(f"删除无主机的宿主机群组: {group_name} (ID: {group_id})")


def run():
    zapi = ZabbixAPI("http://10.20.120.239")
    zapi.login("jiajun.ma", "Abc000425.")

    conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    pgsql = conn.cursor()

    zabbix_obj = Zabbix()

    hosts = pgsql.execute('SELECT * FROM "vCenter_host" WHERE host_connection_state=\'connected\'').fetchall()
    area_gid_dict = {"IT中心管理VCenter": "238", "IT中心云桌面VCenter": "288", "IT中心DMZ区VCenter": "330",
                     "IT中心研发域VCenter": "567", "IT中心测试区VCenter": "672",
                     "IT中心物理内网VCenter": "602", "IT中心物理内网云桌面VCenter": "873"}
    area_proxyid_dict = {"IT中心管理VCenter": "0", "IT中心云桌面VCenter": "0", "IT中心DMZ区VCenter": "0",
                         "IT中心研发域VCenter": "22929", "IT中心测试区VCenter": "0",
                         "IT中心物理内网VCenter": "25941", "IT中心物理内网云桌面VCenter": "25941"}

    for host in hosts:
        # 检查宿主机是否存在于Zabbix
        zabbix_host = zabbix_obj.check_host_exist(host[2], host[3])

        if zabbix_host:
            host_display_name = zabbix_host['name']
            group_id = get_host_group_id(host[3], zabbix_obj)
            group_info = zapi.hostgroup.get(groupids=group_id)[0]

            if group_id is None:
                logger.warn('获取Zabbix主机组ID失败，跳过宿主机: %s' % host[3])
                continue

            # 若宿主机名称不匹配，更新Zabbix中的名称及接口
            if not host[3] in zabbix_host["name"]:
                interfaces = zabbix_host["interfaces"].copy()
                interfaces[0]["ip"] = host[3]
                zabbix_obj.update_host(zabbix_host["hostid"], name=host[2], displayname=host[3],
                                       interface=interfaces, proxy_hostid=area_proxyid_dict[host[0]])
                host_display_name = host[3]
                logger.info('Zabbix主机名称更新为 %s' % host[3])

            host_url, host_uuid = get_macro(zabbix_host["macros"])
            host_vc_url = \
                pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                    0] + "/sdk"

            # 如果宿主机被迁移至其他vCenter，更新Zabbix宏、所属主机组、proxyid数据
            if host_url != host_vc_url or host_uuid != host[2]:
                host_macro = [{"macro": "{$VMWARE.URL}", "value": host_vc_url},
                              {"macro": "{$VMWARE.HV.UUID}", "value": host[2]}]
                host_group_id = [group for group in zabbix_host["groups"] if
                                 group["groupid"] not in area_gid_dict.values()]
                host_group_id.append({"groupid": area_gid_dict[host[0]]})
                zabbix_obj.update_host(zabbix_host["hostid"], macros=host_macro, group_list=host_group_id,
                                       proxy_hostid=area_proxyid_dict[host[0]])
                logger.info('更新宿主机信息')

            # 更新Zabbix主机群组名称
            if group_info["name"] != host_display_name:
                zabbix_obj.update_host_group(group_id, host_display_name)
                logger.info('主机群组重命名为 %s' % host_display_name)

            # 处理宿主机下的虚拟机数据
            if host[0] not in {"IT中心云桌面VCenter", "IT中心测试区VCenter"}:
                vms = pgsql.execute(
                    'SELECT * FROM "vCenter_vm" WHERE host_name=\'%s\' AND vc_name != \'IT中心云桌面VCenter\'' % host[
                        3]).fetchall()
                for vm in vms:
                    zabbix_vm_host = zabbix_obj.check_vm_host_exist(vm[2])

                    if vm[3].startswith('vCLS'):
                        continue

                    if zabbix_vm_host:
                        vm_host_macro = None
                        vm_host_group = None

                        vm_url, vm_uuid = get_macro(zabbix_vm_host["macros"])
                        vm_vc_url = pgsql.execute(
                            'SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vm[0]).fetchall()[0][
                                        0] + "/sdk"

                        if zabbix_vm_host["name"] != vm[3] or vm_url != vm_vc_url or vm_uuid != vm[2]:
                            vm_host_macro = [{"macro": "{$VMWARE.URL}", "value": vm_vc_url},
                                             {"macro": "{$VMWARE.VM.UUID}", "value": vm[2]}]

                        vm_group_info = zabbix_vm_host['groups']

                        # 检查是否需要更新宿主机群组
                        host_group_exists = False
                        for group in vm_group_info:
                            if host[3] in group['name']:
                                host_group_exists = True
                                break

                        # 初始化 vm_host_group 为 None
                        vm_host_group = None

                        if not host_group_exists:
                            remove_group_id = None
                            for group in vm_group_info:
                                if '10.' in group['name'] or '192.' in group['name']:
                                    remove_group_id = group['groupid']
                                    break

                            vm_host_group = [{"groupid": group["groupid"]} for group in zabbix_vm_host["groups"]]
                            if remove_group_id:
                                vm_host_group.remove({"groupid": remove_group_id})
                            vm_host_group.append({"groupid": group_id})

                        # 如果是IT中心物理内网云桌面VCenter，检查是否需要添加群组ID 1165
                        if host[0] == "IT中心物理内网云桌面VCenter":
                            # 检查是否已经包含群组ID 1165
                            if not any(group["groupid"] == "1165" for group in vm_group_info):
                                # 如果之前没有创建 vm_host_group，现在创建
                                if vm_host_group is None:
                                    vm_host_group = [{"groupid": group["groupid"]} for group in
                                                     zabbix_vm_host["groups"]]
                                # 添加群组 1165
                                vm_host_group.append({"groupid": "1165"})

                        if vm_host_macro or vm_host_group:
                            update_args = {
                                "hostid": zabbix_vm_host["hostid"],
                                "displayname": vm[3],
                            }
                            if vm_host_macro:
                                update_args["macros"] = vm_host_macro
                            if vm_host_group:
                                update_args["group_list"] = vm_host_group
                            zabbix_obj.update_host(**update_args)

                            update_parts = []
                            if vm_host_macro:
                                update_parts.append("宏")
                            if vm_host_group:
                                update_parts.append("群组")
                            logger.info('更新 %s 虚拟机信息: %s' % (vm[3], ','.join(update_parts)))

                        # 如果是IT中心物理内网云桌面VCenter，检查并关联模板27097
                        if host[0] == "IT中心物理内网云桌面VCenter":
                            current_templates = zabbix_vm_host.get('parentTemplates', [])
                            if not any(template.get('templateid') == "27097" for template in current_templates):
                                # 获取当前所有模板ID
                                template_ids = [template["templateid"] for template in current_templates]
                                template_ids.append("27097")
                                # 使用 zapi 直接调用 host.update 来更新模板
                                zapi.host.update(
                                    hostid=zabbix_vm_host["hostid"],
                                    templates=[{"templateid": tid} for tid in template_ids]
                                )
                                logger.info('为虚拟机 %s 关联模板27097' % vm[3])

                    else:
                        # 创建新的虚拟机主机
                        interface = [{"type": 1, "main": 1, "useip": 1, "ip": vm[4], "dns": "", "port": "10050"}]
                        vm_vc_url = pgsql.execute(
                            'SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % vm[0]).fetchall()[0][
                                        0] + "/sdk"
                        vm_host_macro = [{"macro": "{$VMWARE.URL}", "value": vm_vc_url},
                                         {"macro": "{$VMWARE.VM.UUID}", "value": vm[2]}]

                        # 如果是IT中心物理内网云桌面VCenter，需要同时添加到两个群组
                        if host[0] == "IT中心物理内网云桌面VCenter":
                            # 创建包含两个群组的列表
                            group_list = [{"groupid": group_id}, {"groupid": "1165"}]
                            # 先创建主机
                            result = zabbix_obj.create_host(vm[2], vm[3], group_list, 10124, interface, vm_host_macro,
                                                            area_proxyid_dict[host[0]])
                            # 获取新创建的主机ID并关联额外模板
                            new_host_id = result["result"]["hostids"][0]
                            # 使用 zapi 关联模板27097
                            zapi.host.update(
                                hostid=new_host_id,
                                templates=[{"templateid": "10124"}, {"templateid": "27097"}]
                            )
                            logger.info('虚拟机 %s 主机创建成功并关联模板27097' % vm[3])
                        else:
                            # 其他vCenter只添加到宿主机群组
                            zabbix_obj.create_host(vm[2], vm[3], group_id, 10124, interface, vm_host_macro,
                                                   area_proxyid_dict[host[0]])
                            logger.info('虚拟机 %s 主机创建成功' % vm[3])

        else:
            # 创建宿主机
            interface = [{"type": 1, "main": 1, "useip": 1, "ip": host[3], "dns": "", "port": "443"},
                         {"type": 2, "main": 1, "useip": 1, "ip": host[3], "dns": "", "port": "161",
                          "details": {"version": 2, "bulk": 1, "community": "{$SNMP_COMMUNITY}"}}]
            host_vc_url = \
                pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][
                    0] + "/sdk"
            host_macro = [{"macro": "{$VMWARE.URL}", "value": host_vc_url},
                          {"macro": "{$VMWARE.HV.UUID}", "value": host[2]}]
            zabbix_obj.create_host(host[2], host[3], area_gid_dict[host[0]], 10123, interface, host_macro,
                                   area_proxyid_dict[host[0]])
            logger.info('%s 宿主机创建成功' % host[3])

    cleanup_unused_host_groups(zabbix_obj, area_gid_dict)

    pgsql.close()
    conn.close()


def main():
    logger.info('开始同步PostgreSQL数据到zabbix监控平台')
    start_time = time.time()

    run()

    end_time = time.time()
    process_time = end_time - start_time
    logger.info("PostgreSQL数据同步到zabbix监控平台执行完成。耗时：%.1f 秒" % process_time)


if __name__ == '__main__':
    main()
