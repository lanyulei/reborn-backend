package data

import (
	"fmt"
	"path/filepath"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"

	"github.com/google/wire"
)

// ProviderSet is file 域 data providers.
var ProviderSet = wire.NewSet(
	NewFileRepo,
	NewStores,
	wire.Bind(new(biz.Repo), new(*fileRepo)),
	wire.Bind(new(biz.BindRepo), new(*fileRepo)),
	wire.Bind(new(biz.PurgeRepo), new(*fileRepo)),
)

// localSubDir 附件在上传根目录下的子目录（与 logo / 头像分开）。
const localSubDir = "files"

// s3DefaultRegion 未配置区域时的缺省值（MinIO 等不分区域的服务的默认区域）。
const s3DefaultRegion = "us-east-1"

// NewStores 按配置装配存储驱动：本地驱动恒登记，s3 驱动在配置了 endpoint 时登记——每个文件
// 记录自己的驱动，切换默认驱动后旧文件仍按原驱动读取。upload.driver 留空取 local；取值未登记
// （含选了 s3 却没配）或 s3 配置不全时，装配阶段快速失败。新增驱动在此登记。
func NewStores(c *conf.Data, d *basedata.Data) (biz.Stores, error) {
	drivers := map[string]storage.ObjectStore{
		DriverLocal: NewLocalStore(filepath.Join(d.LogoDir, localSubDir)),
	}
	if s3 := c.GetUpload().GetS3(); s3.GetEndpoint() != "" {
		region := s3.GetRegion()
		if region == "" {
			region = s3DefaultRegion
		}
		store, err := NewS3Store(S3Config{
			Endpoint: s3.GetEndpoint(), Region: region, Bucket: s3.GetBucket(),
			AccessKey: s3.GetAccessKey(), SecretKey: s3.GetSecretKey(),
			PathStyle: s3.GetPathStyle(), Prefix: s3.GetPrefix(),
		}, biz.MaxFileSize)
		if err != nil {
			return biz.Stores{}, fmt.Errorf("upload.s3 (set KRATOS_DATA_UPLOAD_S3_ACCESS_KEY / _SECRET_KEY): %w", err)
		}
		drivers[DriverS3] = store
	}
	def := c.GetUpload().GetDriver()
	if def == "" {
		def = DriverLocal
	}
	if _, ok := drivers[def]; !ok {
		return biz.Stores{}, fmt.Errorf("unsupported upload driver %q", def)
	}
	return biz.Stores{Default: def, Drivers: drivers}, nil
}
