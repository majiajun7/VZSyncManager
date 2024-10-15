import re
import psycopg
from zabbix import Zabbix

def connect_to_db():
    conn = psycopg.connect("host=10.20.120.239 dbname=script user=postgres password=^cA&PVp4rrR3Tvs^HPiQ")
    pgsql = conn.cursor()
    return conn, pgsql

def get_hosts(pgsql):
    return pgsql.execute('SELECT * FROM "vCenter_host" WHERE host_connection_state=\'CONNECTED\'').fetchall()

def get_area_gid_dict():
    return {"IT中心管理VCenter": "238", "IT中心云桌面VCenter": "288", "IT中心DMZ区VCenter": "330",
            "IT中心综合业务区VCenter": "329", "IT中心研发域VCenter": "567"}

def get_group_info(zabbix_obj):
    return zabbix_obj.get_host_group_all()

def get_group_id(zabbix_obj, host, group_info):
    for group in group_info["result"]:
        if re.search("%s$" % host[3], group["name"]):
            return group["groupid"]
    return zabbix_obj.create_host_group(host[3])["result"]["groupids"][0]

def get_zabbix_host(zabbix_obj, host):
    return zabbix_obj.check_host_exist(host[2], host[3])

def update_host_name(zabbix_obj, host, zabbix_host):
    if re.search(".*%s.*" % host[3], zabbix_host["name"]) is None:
        zabbix_obj.update_host(zabbix_host["hostid"], name=host[2], displayname=host[3])
        return True
    return False

def update_group_name(zabbix_obj, group_info, zabbix_host):
    ipaddress = zabbix_host["interfaces"][0]["ip"]
    for group in group_info["result"]:
        if re.search(".*%s$" % ipaddress, group["name"]):
            if zabbix_host['name'] != group["name"]:
                group_id = group["groupid"]
                zabbix_obj.update_host_group(group_id, zabbix_host["name"])
                return group_id
    return None

def get_macro_value(zabbix_host, macro_key):
    for i in zabbix_host["macros"]:
        if i["macro"] == macro_key:
            return i["value"]
    return ""

def get_vc_url(pgsql, host):
    return str(pgsql.execute('SELECT vc_url FROM "vCenter_certficate" WHERE vc_name=\'%s\'' % host[0]).fetchall()[0][0]) + "/sdk"

def check_macro_update(pgsql, host, zabbix_host):
    result = get_macro_value(zabbix_host, "{$VMWARE.HV.UUID}")
    result1 = get_macro_value(zabbix_host, "{$VMWARE.URL}")
    vc_url = get_vc_url(pgsql, host)
    if not (result == host[2] and result1 == vc_url):
        host_macro = [{
            "macro": "{$VMWARE.URL}",
            "value": vc_url
        },
            {
                "macro": "{$VMWARE.HV.UUID}",
                "value": host[2],
            }]
        return host_macro
    return []

# 以下省略其他函数定义，以及更新main函数以适应新的函数定义。

def main():
    conn, pgsql = connect_to_db()
    hosts = get_hosts(pgsql)
    zabbix_obj = Zabbix()
    group_info = get_group_info(zabbix_obj)
    area_gid_dict = get_area_gid_dict()

    for host in hosts:
        group_id = get_group_id(zabbix_obj, host, group_info)
        zabbix_host = get_zabbix_host(zabbix_obj, host)
        if not zabbix_host:
            zabbix_host = zabbix_obj.create_host(
                host[2],
                host[3],
                group_id,
                host[1],
                area_gid_dict[host[3]]
            )
        else:
            update_host_name(zabbix_obj, host, zabbix_host)
            update_group_name(zabbix_obj, group_info, zabbix_host)

        host_macro = check_macro_update(pgsql, host, zabbix_host)
        if host_macro:
            zabbix_obj.update_host_macro(zabbix_host["hostid"], host_macro)

    conn.close()

if __name__ == "__main__":
    main()

