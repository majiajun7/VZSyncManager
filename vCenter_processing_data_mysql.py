from vCenter_tools import *


mysql = Mysql("10.20.120.240", "jiajun.ma", "Abc000425.", "test")
certificate_tup = mysql.select_data("SELECT * FROM vCenter_certficate")
vcenter_obj_list = [Vcenter(certificate[1], certificate[2], certificate[3], certificate[0]) for certificate in certificate_tup]
for vcenter_obj in vcenter_obj_list:
    print(vcenter_obj)
    run = Process(vcenter_obj)
    run.start_sync()
