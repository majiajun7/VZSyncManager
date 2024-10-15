import logging
from datetime import timedelta
import timeout_decorator
from pyVim import connect
from pyVmomi import vim
import psycopg2
import fcntl


class Process:
    def __init__(self):
        self.Hosts = []

    def process_folder(self, obj):
        for host in obj.childEntity:
            if "ComputeResource" in str(type(host)):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
            elif "ClusterComputeResource" in str(type(host)):
                self.process_clustercomputeresource(host)
            elif "Datacenter" in str(host):
                self.process_datacenter(host)

    def process_datacenter(self, obj):
        for host in obj.hostFolder.childEntity:
            if "ClusterComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
            elif "ComputeResource" in str(host):
                for HostSystem in host.host:
                    self.Hosts.append(HostSystem)
            elif "Folder" in str(host):
                self.process_folder(host)

    def process_clustercomputeresource(self, obj):
        for HostSystem in obj.host:
            self.Hosts.append(HostSystem)

    def process_root(self, si_obj):
        for datacenter in si_obj.content.rootFolder.childEntity:
            if "Datacenter" in str(datacenter):
                self.process_datacenter(datacenter)
            elif "Folder" in str(datacenter):
                self.process_folder(datacenter)

    def get_hostsystem_obj(self, si_obj):
        self.process_root(si_obj)
        return self.Hosts


@timeout_decorator.timeout(5)
def connect_to_vcenter(host, user, pwd):
    return connect.SmartConnect(host=host, user=user, pwd=pwd, disableSslCertValidation=True)


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level='INFO')
    try:
        # 获取文件锁，保证同时只有一个该程序在运行
        lock_file = open('program.lock', 'w')
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.error('已经有相同的程序在运行了，程序即将退出')
        exit()
    else:
        logging.info('文件锁获取成功，开始执行代码')

    conn = psycopg2.connect(host='10.20.120.239', user='postgres', password='^cA&PVp4rrR3Tvs^HPiQ', dbname='script')
    cur = conn.cursor()
    cur.execute('truncate table "vCenter_host_disk_io"')
    cur.execute('SELECT * FROM "vCenter_certficate"')
    for host in cur.fetchall():
        try:
            ServiceInstance = connect_to_vcenter(host=host[1][8:], user=host[2], pwd=host[3])
        except timeout_decorator.TimeoutError:
            logging.warning("连接 %s vCenter超过5秒，自动跳过" % host[1][8:])
            continue
        except Exception:
            logging.warning("连接 %s vCenter失败，自动跳过" % host[1][8:])
            continue

        try:
            run = Process()
            HostSystem_list = run.get_hostsystem_obj(ServiceInstance)
            vc_time = ServiceInstance.CurrentTime()
            start_time = vc_time - timedelta(minutes=1)

            PerfCounterInfo = ServiceInstance.content.perfManager.perfCounter
            Counter_dict = {}
            for Counter in PerfCounterInfo:
                Counter_dict["%s.%s.%s" % (Counter.groupInfo.key, Counter.nameInfo.key, Counter.rollupType)] = Counter.key

            metric_id = [vim.PerformanceManager.MetricId(counterId=Counter_dict["disk.read.average"], instance=""),
                         vim.PerformanceManager.MetricId(counterId=Counter_dict["disk.write.average"], instance="")]

            for HostSystem in HostSystem_list:
                work = False
                QuerySpec = vim.PerformanceManager.QuerySpec(intervalId=20, entity=HostSystem, metricId=metric_id,
                                                             startTime=start_time, endTime=vc_time)

                import eventlet
                eventlet.monkey_patch()

                with eventlet.Timeout(3, False):
                    entity = ServiceInstance.content.perfManager.QueryPerf(querySpec=[QuerySpec])
                    work = True

                if not work:
                    logging.warning('获取宿主机性能指标超过3秒，跳过 %s' % HostSystem.name)
                    continue

                try:
                    cur.execute('INSERT INTO "vCenter_host_disk_io" VALUES (%s, %s, %s, %s, %s)', (
                    HostSystem.name, HostSystem.hardware.systemInfo.uuid, entity[0].value[0].value[-1],
                    entity[0].value[1].value[-1], host[0]))
                except Exception:
                    cur.execute('INSERT INTO "vCenter_host_disk_io" VALUES (%s, %s, %s, %s, %s)',
                                (HostSystem.name, HostSystem.hardware.systemInfo.uuid, "0", "0", host[0]))
        except Exception as e:
            logging.error('获取宿主机磁盘性能过程中发生错误：%s' % e)

    # 统一写入数据库
    conn.commit()

    cur.close()
    conn.close()

    logging.info('程序执行完毕，释放文件锁')
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    lock_file.close()


if __name__ == '__main__':
    main()
