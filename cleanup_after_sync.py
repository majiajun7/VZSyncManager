#!/usr/bin/env python3
"""
同步后清理脚本
在 start_sync_data.py 执行完成后运行，清理无效的 Zabbix 主机
"""

import requests
import sys
import time
import json
import logging
from datetime import datetime
import os

# 配置日志
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cleanup_after_sync.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Zabbix 配置
ZABBIX_URL = "http://10.20.120.239"
ZABBIX_API_URL = f"{ZABBIX_URL}/api_jsonrpc.php"
ZABBIX_USER = "jiajun.ma"
ZABBIX_PASS = "Abc000425."

# 指定的模板ID - 只删除仅关联此模板的主机
SPECIFIC_TEMPLATE_ID = "10124"  # 请修改为你需要的模板ID


def get_auth_token():
    """获取 Zabbix API 认证令牌"""
    payload = {
        "jsonrpc": "2.0",
        "method": "user.login",
        "params": {
            "user": ZABBIX_USER,
            "password": ZABBIX_PASS
        },
        "id": 1
    }

    try:
        response = requests.post(ZABBIX_API_URL, json=payload, timeout=30)
        result = response.json()

        if "error" in result:
            logger.error(f"认证失败: {result['error']['data']}")
            return None

        return result["result"]
    except Exception as e:
        logger.error(f"连接 Zabbix API 失败: {e}")
        return None


def get_hosts_with_problems(auth_token):
    """获取有数据获取问题的主机"""
    payload = {
        "jsonrpc": "2.0",
        "method": "problem.get",
        "params": {
            "output": ["eventid", "objectid", "name"],
            "search": {
                "name": "Zabbix cant't get data (for 3m ）(or nodata for 3m )"
            },
            "selectHosts": ["hostid", "host", "name", "status"],
            "sortfield": ["eventid"],
            "sortorder": "DESC"
        },
        "auth": auth_token,
        "id": 2
    }

    try:
        response = requests.post(ZABBIX_API_URL, json=payload, timeout=30)
        result = response.json()

        if "error" in result:
            logger.error(f"获取问题列表失败: {result['error']['data']}")
            return []

        # 收集所有受影响的主机（去重）
        hosts_dict = {}
        problems = result["result"]

        for problem in problems:
            # 通过触发器获取主机信息
            trigger_payload = {
                "jsonrpc": "2.0",
                "method": "trigger.get",
                "params": {
                    "triggerids": problem["objectid"],
                    "output": ["triggerid"],
                    "selectHosts": ["hostid", "host", "name", "status"]
                },
                "auth": auth_token,
                "id": 3
            }

            trigger_response = requests.post(ZABBIX_API_URL, json=trigger_payload, timeout=30)
            trigger_result = trigger_response.json()

            if "result" in trigger_result and trigger_result["result"]:
                for trigger in trigger_result["result"]:
                    if "hosts" in trigger:
                        for host in trigger["hosts"]:
                            hosts_dict[host["hostid"]] = host

        return list(hosts_dict.values())
    except Exception as e:
        logger.error(f"获取主机列表失败: {e}")
        return []


def check_host_templates(auth_token, hostid):
    """检查主机是否仅关联了指定的模板"""
    payload = {
        "jsonrpc": "2.0",
        "method": "host.get",
        "params": {
            "hostids": hostid,
            "output": ["hostid"],
            "selectParentTemplates": ["templateid", "name"]
        },
        "auth": auth_token,
        "id": 6
    }
    
    try:
        response = requests.post(ZABBIX_API_URL, json=payload, timeout=30)
        result = response.json()
        
        if "error" in result or not result.get("result"):
            return False
        
        host_data = result["result"][0]
        templates = host_data.get("parentTemplates", [])
        
        # 检查是否只有一个模板，且该模板ID等于指定值
        if len(templates) == 1 and templates[0]["templateid"] == SPECIFIC_TEMPLATE_ID:
            return True
        
        return False
    except Exception as e:
        logger.error(f"检查主机模板失败: {e}")
        return False


def delete_hosts_batch(auth_token, host_ids):
    """批量删除主机"""
    payload = {
        "jsonrpc": "2.0",
        "method": "host.delete",
        "params": host_ids,
        "auth": auth_token,
        "id": 4
    }

    try:
        response = requests.post(ZABBIX_API_URL, json=payload, timeout=60)
        result = response.json()

        if "error" in result:
            logger.error(f"批量删除失败: {result['error']['data']}")
            return 0

        return len(host_ids)
    except Exception as e:
        logger.error(f"删除请求失败: {e}")
        return 0


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始执行同步后清理任务")
    logger.info(f"Zabbix 服务器: {ZABBIX_URL}")
    logger.info(f"删除条件: 仅关联模板ID {SPECIFIC_TEMPLATE_ID} 的主机")
    logger.info("=" * 60)

    # 获取认证令牌
    auth_token = get_auth_token()
    if not auth_token:
        logger.error("无法获取 Zabbix 认证令牌，退出")
        sys.exit(1)

    # 获取有问题的主机
    problem_hosts = get_hosts_with_problems(auth_token)
    logger.info(f"找到 {len(problem_hosts)} 个有数据获取问题的主机")

    if not problem_hosts:
        logger.info("没有需要清理的主机")
        return

    # 过滤需要删除的主机
    hosts_to_delete = []
    hosts_checked = 0
    
    for host in problem_hosts:
        hosts_checked += 1
        if hosts_checked % 10 == 0:
            logger.info(f"已检查 {hosts_checked}/{len(problem_hosts)} 个主机")
        
        # 检查是否只关联了指定模板
        if not check_host_templates(auth_token, host['hostid']):
            continue
        
        # 如果通过了模板检查，添加到删除列表
        hosts_to_delete.append(host)

    logger.info(f"过滤后需要删除 {len(hosts_to_delete)} 个主机")

    if not hosts_to_delete:
        logger.info("没有符合删除条件的主机")
        return

    # 记录将要删除的主机
    logger.info("将要删除的主机列表：")
    for i, host in enumerate(hosts_to_delete[:20]):  # 只记录前20个
        logger.info(f"  {i+1}. {host['host']} (ID: {host['hostid']})")
    if len(hosts_to_delete) > 20:
        logger.info(f"  ... 还有 {len(hosts_to_delete) - 20} 个主机")

    # 批量删除主机
    batch_size = 50
    total_deleted = 0
    
    for i in range(0, len(hosts_to_delete), batch_size):
        batch = hosts_to_delete[i:i + batch_size]
        host_ids = [h['hostid'] for h in batch]
        
        logger.info(f"删除第 {i//batch_size + 1} 批，共 {len(host_ids)} 个主机")
        deleted = delete_hosts_batch(auth_token, host_ids)
        total_deleted += deleted
        
        if deleted < len(host_ids):
            logger.warning(f"批次删除不完全：期望删除 {len(host_ids)}，实际删除 {deleted}")
        
        # 避免请求过快
        if i + batch_size < len(hosts_to_delete):
            time.sleep(1)

    # 显示统计
    logger.info("=" * 60)
    logger.info("清理任务完成")
    logger.info(f"发现问题主机: {len(problem_hosts)}")
    logger.info(f"符合删除条件: {len(hosts_to_delete)}")
    logger.info(f"成功删除: {total_deleted}")
    logger.info(f"删除失败: {len(hosts_to_delete) - total_deleted}")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n用户中断执行")
        sys.exit(1)
    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        sys.exit(1)