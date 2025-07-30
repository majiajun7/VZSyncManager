-- 为vCenter虚拟机表添加自定义字段
-- 执行前请确保已连接到正确的数据库

-- 为vCenter_vm表添加三个新字段
ALTER TABLE "vCenter_vm" 
ADD COLUMN IF NOT EXISTS "cmdb_id" VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS "vm_owner" VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS "department" VARCHAR(200) DEFAULT '';

-- 为vCenter_vm_archive表添加相同的三个字段（归档表）
ALTER TABLE "vCenter_vm_archive" 
ADD COLUMN IF NOT EXISTS "cmdb_id" VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS "vm_owner" VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS "department" VARCHAR(200) DEFAULT '';

-- 添加注释说明字段用途
COMMENT ON COLUMN "vCenter_vm"."cmdb_id" IS '资产编号';
COMMENT ON COLUMN "vCenter_vm"."vm_owner" IS '资源使用人';
COMMENT ON COLUMN "vCenter_vm"."department" IS '使用人部门';

COMMENT ON COLUMN "vCenter_vm_archive"."cmdb_id" IS '资产编号';
COMMENT ON COLUMN "vCenter_vm_archive"."vm_owner" IS '资源使用人';
COMMENT ON COLUMN "vCenter_vm_archive"."department" IS '使用人部门';

-- 查询验证新字段是否添加成功
SELECT column_name, data_type, character_maximum_length, column_default 
FROM information_schema.columns 
WHERE table_name IN ('vCenter_vm', 'vCenter_vm_archive') 
  AND column_name IN ('cmdb_id', 'vm_owner', 'department')
ORDER BY table_name, column_name;