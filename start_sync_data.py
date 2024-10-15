if __name__ == '__main__':
    import send_message
    import vcenter_to_pgsql_sync
    import pgsql_to_zabbix_sync
    import zabbix_vcenter_data_sync_external

    vcenter_to_pgsql_sync.main()
    pgsql_to_zabbix_sync.main()
    zabbix_vcenter_data_sync_external.main()

    message_content = '\n'.join(vcenter_to_pgsql_sync.logger.handlers[0].logs + pgsql_to_zabbix_sync.logger.handlers[0].logs + zabbix_vcenter_data_sync_external.logger.handlers[0].logs)
    send_message.dingtalk(message_content)
