-- 0039 用户头像：保存头像访问地址（本地上传后为 /api/v1/user/{id}/avatar?v=<指纹>；空 = 未设置）。
ALTER TABLE "system_user" ADD COLUMN IF NOT EXISTS avatar varchar(255) NOT NULL DEFAULT '';
COMMENT ON COLUMN "system_user".avatar IS '头像地址；本地上传时为带内容指纹的服务路径，空串 = 未设置';
