import ssl
from pyVim import connect


def process_folder(obj):
    for host in obj.childEntity:
        if "ComputeResource" in str(type(host)):
            for HostSystem in host.host:
                Hosts.append(HostSystem)
        elif "ClusterComputeResource" in str(type(host)):
            process_clustercomputeresource(host)
            # print(host)
        elif "Datacenter" in str(host):
            process_datacenter(host)
            # print(host)


def process_datacenter(obj):
    # if hasattr(obj, "hostFolder"):
    for host in obj.hostFolder.childEntity:
        # print(host.name)
        if "ClusterComputeResource" in str(host):
            for HostSystem in host.host:
                Hosts.append(HostSystem)
                # print(HostSystem)
        elif "ComputeResource" in str(host):
            for HostSystem in host.host:
                Hosts.append(HostSystem)
                # print(HostSystem)
        elif "Folder" in str(host):
            process_folder(host)
            # print(host)


def process_clustercomputeresource(obj):
    for HostSystem in obj.host:
        Hosts.append(HostSystem)


def process_root():
    for datacenter in content.rootFolder.childEntity:
        if "Datacenter" in str(datacenter):
            process_datacenter(datacenter)
            # print(datacenter)
        elif "Folder" in str(datacenter):
            process_folder(datacenter)
            # print(datacenter)


ssl._create_default_https_context = ssl._create_unverified_context

# ServiceInstance = connect.SmartConnect(host='10.20.120.56', user='monitor@vsphere.local', pwd='n.-68r_d*&u+Fq?G', port=443)
ServiceInstance = connect.SmartConnect(host='10.50.61.2', user='monitor@vsphere.local', pwd='n.-68r_d*&u+Fq?G')
# ServiceInstance.RetrieveContent()  # 检索内容
content = ServiceInstance.content
Hosts = []

process_root()
for HostSystem in Hosts:
    print(HostSystem.name, HostSystem.hardware.systemInfo.uuid)
print(len(Hosts))
