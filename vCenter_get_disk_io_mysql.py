from datetime import timedelta
from pyVim import connect
from pyVmomi import vim
import pymysql

class DataRetrieval:
    def __init__(self):
        self.Hosts = []

    def process_folder(self, obj):
        for host in obj.childEntity:
            if "ComputeResource" in str(type(host)):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
            elif "ClusterComputeResource" in str(type(host)):
                self.process_clustercomputeresource(host)
                # print(host)
            elif "Datacenter" in str(host):
                self.process_datacenter(host)
                # print(host)

    def process_datacenter(self, obj):
        # if hasattr(obj, "hostFolder"):
        for host in obj.hostFolder.childEntity:
            # print(host.name)
            if "ClusterComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
                    # print(HostSystem)
            elif "ComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
                    # print(HostSystem)
            elif "Folder" in str(host):
                self.process_folder(host)
                # print(host)

    def process_clustercomputeresource(self, obj):
        for HostSystem in obj.host:
            self.Hosts.append(HostSystem)

    def process_root(self, si_obj):
        for datacenter in si_obj.content.rootFolder.childEntity:
            if "Datacenter" in str(datacenter):
                self.process_datacenter(datacenter)
                # print(datacenter)
            elif "Folder" in str(datacenter):
                self.process_folder(datacenter)

    def get_hostsystem_obj(self, si_obj):
        self.process_root(si_obj)
        return self.Hosts



conn = pymysql.connect(host='10.50.67.51', user='root', password='PlnUOPpDMk', port=32001, database='script')
cur = conn.cursor()

cur.execute('truncate table vCenter_host_disk_io')
cur.execute('SELECT * FROM vCenter_certficate')
# print(cur.fetchall())
for host in cur.fetchall():
    ServiceInstance = connect.SmartConnect(host=host[1][8:], user=host[2], pwd=host[3], disableSslCertValidation=True)
# username, password, url, uuid, select = sys.argv[1], sys.argv[2], sys.argv[3][8:-4], sys.argv[4], sys.argv[5]
# ServiceInstance = connect.SmartConnect(host=url, user=username, pwd=password, disableSslCertValidation=True)
# ServiceInstance.RetrieveContent()  # 检索内容
    run = DataRetrieval()
    HostSystem_list = run.get_hostsystem_obj(ServiceInstance)
    # for HostSystem_obj in HostSystem_list:
    #     if HostSystem_obj.hardware.systemInfo.uuid == uuid:
    #         HostSystem = HostSystem_obj
            # print(HostSystem.name, HostSystem.hardware.systemInfo.uuid)
    # print(len(HostSystem_list))

    vc_time = ServiceInstance.CurrentTime()
    start_time = vc_time - timedelta(minutes=1)

    # 生成指标对应ID字典
    PerfCounterInfo = ServiceInstance.content.perfManager.perfCounter
    Counter_dict = {}
    for Counter in PerfCounterInfo:
        Counter_dict["%s.%s.%s" % (Counter.groupInfo.key, Counter.nameInfo.key, Counter.rollupType)] = Counter.key
        print(Counter.groupInfo.key, Counter.nameInfo.key, Counter.rollupType, Counter.unitInfo.key, Counter.key)

    metric_id = [vim.PerformanceManager.MetricId(counterId=Counter_dict["disk.read.average"], instance=""),
                 vim.PerformanceManager.MetricId(counterId=Counter_dict["disk.write.average"], instance="")]
    for HostSystem in HostSystem_list:
        QuerySpec = vim.PerformanceManager.QuerySpec(intervalId=20, entity=HostSystem, metricId=metric_id, startTime=start_time, endTime=vc_time)
        entity = ServiceInstance.content.perfManager.QueryPerf(querySpec=[QuerySpec])
        try:
            cur.execute('INSERT INTO vCenter_host_disk_io VALUES (%s, %s, %s, %s, %s)', (HostSystem.name, HostSystem.hardware.systemInfo.uuid, entity[0].value[0].value[-1], entity[0].value[1].value[-1], host[0]))
        except Exception:
            cur.execute('INSERT INTO vCenter_host_disk_io VALUES (%s, %s, %s, %s, %s)', (HostSystem.name, HostSystem.hardware.systemInfo.uuid, "0", "0", host[0]))

        # print(entity[0].value[0].value[-1])
        # print(entity[0].value[1].value[-1])

conn.commit()

cur.close()
conn.close()
