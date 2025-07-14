import urllib
import urllib.parse

from pyVim import connect
from pyVmomi import vim


class Vcenter:
    """
    这个类负责管理与vCenter的连接，并按照一定的格式处理获取到的数据。
    不再依赖 vSphere RESTful API，而是改用 pyVmomi。
    """

    def __init__(self, host, username, password, name):
        """
        初始化并连接到 vCenter。此时会收集必要的信息，供后续方法使用。
        :param host: vCenter 主机地址（例如 https://x.x.x.x）
        :param username: vCenter 用户名
        :param password: vCenter 密码
        :param name: 给该 vCenter 自定义的一个名字（self.name）
        """
        self.name = name

        # 如果传进来的 host 带有 "https://" 前缀，可做简单剥离
        # （若已保证外部一定是纯 IP 或域名，则可以不做这步处理）
        cleaned_host = host.replace("https://", "").rstrip('/')

        # 通过 pyVmomi 连接到 vCenter
        self.si = connect.SmartConnect(
            host=cleaned_host,
            user=username,
            pwd=password,
            port=443,
            disableSslCertValidation=True
        )
        self.content = self.si.content

        # 通过 ContainerView 获取所有数据中心、宿主机和虚拟机对象
        self.dc_view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.Datacenter], True
        )
        self.datacenters = list(self.dc_view.view)  # 所有 Datacenter 对象

        self.host_view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.HostSystem], True
        )
        self.hosts = list(self.host_view.view)  # 所有 HostSystem 对象

        self.vm_view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.VirtualMachine], True
        )
        self.vms = list(self.vm_view.view)  # 所有 VM 对象

    def get_datacenter(self) -> list:
        """
        返回数据中心信息，结构与原先第二个文件中相同：
        [
            {
                "datacenter": "datacenter-xxx",  # Datacenter 的 managed object ID
                "name": "DatacenterName"
            },
            ...
        ]
        """
        result = []
        for dc in self.datacenters:
            result.append({
                "datacenter": dc._moId,  # Datacenter 的 MoRef ID
                "name": urllib.parse.unquote(dc.name)  # 原逻辑中对 name 做过 unquote，这里保持一致
            })
        return result

    def get_host(self, datacenter_id) -> list:
        """
        根据给定的 datacenter_id (即 datacenter._moId)，获取属于该数据中心的宿主机信息。
        返回结构如下：
        [
            {
                "host": "host-xxx",  # Host 的 MoRef ID
                "name": "192.168.xxx.xxx",  # HostSystem.name
                "connection_state": "CONNECTED" or "DISCONNECTED" ...
                "power_state": "poweredOn"/"poweredOff"/"standBy"... (若无则为空字符串)
                "uuid": "xxxx-xxxx-xxxx-xxxx"  # host.hardware.systemInfo.uuid
            },
            ...
        ]
        """
        results = []
        for host in self.hosts:
            # 判断该宿主机所在的 Datacenter 是否与 datacenter_id 相匹配
            host_dc = self._find_datacenter_of_host(host)
            if host_dc and host_dc._moId == datacenter_id:
                connection_state = str(host.summary.runtime.connectionState)
                power_state = str(host.summary.runtime.powerState) \
                    if host.summary.runtime.powerState else ""

                results.append({
                    "host": host._moId,
                    "name": host.name,
                    "connection_state": connection_state,
                    "power_state": power_state,
                    "uuid": host.hardware.systemInfo.uuid
                })
        return results

    def get_vm(self, host_id) -> list:
        """
        根据给定的 host_id (即 host._moId)，获取该宿主机下的所有虚拟机信息。
        返回结构如下：
        [
            {
                "vm": "vm-xxx",       # VM 的 MoRef ID
                "name": "VMName",
                "power_state": "POWERED_ON"/"POWERED_OFF"/"SUSPENDED",
                "cpu_count": 2,
                "memory_size_MiB": 4096,
                "uuid": "vm-uuid",
                "ipaddress": "x.x.x.x",
                "annotation": "备注信息"
            },
            ...
        ]
        """
        results = []
        the_host = None
        for host in self.hosts:
            if host._moId == host_id:
                the_host = host
                break

        # 如果找不到对应 host，则返回空列表
        if not the_host:
            return results

        # 遍历该宿主机下所有 vm
        for vm in the_host.vm:
            # 跳过模板类型的虚拟机
            if vm.summary.config.template:
                continue
                
            power_state = str(vm.runtime.powerState) if vm.runtime.powerState else ""

            # 虚拟机 CPU / 内存等信息
            cpu_count = vm.summary.config.numCpu if vm.summary.config.numCpu else 0
            memory_size = vm.summary.config.memorySizeMB if vm.summary.config.memorySizeMB else 0

            results.append({
                "vm": vm._moId,
                "name": vm.name,
                "power_state": power_state,
                "cpu_count": cpu_count,
                "memory_size_MiB": memory_size,
                "uuid": vm.summary.config.instanceUuid or "",
                # 获取 IP 地址
                "ipaddress": vm.guest.ipAddress if vm.guest.ipAddress else "0.0.0.0",
                # 获取备注信息
                "annotation": vm.summary.config.annotation or ""
            })
        return results

    def get_vm_uuid(self, vm_id):
        """
        通过 VM 的 MoRef ID 获取其 instanceUuid
        """
        for vm in self.vms:
            if vm._moId == vm_id:
                if vm.summary.config and vm.summary.config.instanceUuid:
                    return vm.summary.config.instanceUuid
        return ""

    def get_vm_ipaddress(self, vm_id):
        """
        通过 VM 的 MoRef ID 获取其 IP 地址
        """
        for vm in self.vms:
            if vm._moId == vm_id:
                return vm.guest.ipAddress or "0.0.0.0"
        return "0.0.0.0"

    def get_host_uuid(self, ipaddress):
        """
        根据 Host 的 name（IP地址或主机名）来获取 hardware.systemInfo.uuid
        """
        for host in self.hosts:
            if host.name == ipaddress:
                return host.hardware.systemInfo.uuid
        return ""

    def get_vm_annotation(self, vm_uuid):
        """
        使用 instanceUuid 来获取虚拟机的备注信息 (annotation)。
        """
        for vm in self.vms:
            if vm.summary.config.instanceUuid == vm_uuid:
                return vm.summary.config.annotation or ""
        return ""

    def _find_datacenter_of_host(self, host):
        """
        向上遍历宿主机的父节点，直到找到其所在 Datacenter。
        """
        parent = host.parent
        while parent:
            if isinstance(parent, vim.Datacenter):
                return parent
            parent = parent.parent
        return None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"
