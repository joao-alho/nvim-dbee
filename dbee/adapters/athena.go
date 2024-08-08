package adapters

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
	drv "github.com/uber/athenadriver/go"
)

func init() {
	_ = register(&Athena{}, "athena", "awsathena")
}

var _ core.Adapter = (*Athena)(nil)

type Athena struct{}

// The format of the url is as follows:
//
//	athena://[region][?options]
//
// Where:
//   - `region` is mandatory. It should match the intended
//     AWS Region to run queries on
//   - `options` is a & delimited list of key=value arguments.
//
// The supported `options` are:
//   - `work_group` define an Athena workgroup to run queries. default=default
//   - `s3_staging_dir` S3 bucket and path where Athena stores results and metadata
//
// #TODO: Implement actions, currently only supports read only mode
func (a *Athena) Connect(rawUrl string) (core.Driver, error) {
	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	conf := drv.NewNoOpsConfig()
	conf.SetReadOnly(true)

	if u.Scheme != "athena" {
		return nil, fmt.Errorf("unexpected scheme: %q", u.Scheme)
	}

	if u.Host != "" {
		conf.SetRegion(u.Host)
	} else {
		return nil, fmt.Errorf("missing region in url: %s", rawUrl)
	}

	params := u.Query()
	if params.Get("work_group") == "" && params.Get("s3_staging_dir") == "" {
		return nil, fmt.Errorf("one of work_group or s3_staging_dir must be set in: %s", rawUrl)
	}
	if params.Get("work_group") != "" {
		wg := drv.NewWG(params.Get("work_group"), nil, nil)
		conf.SetWorkGroup(wg)
		conf.SetWGRemoteCreationAllowed(false)
	}
	if params.Get("s3_staging_dir") != "" {
		bucket := params.Get("s3_staging_dir")
		conf.SetOutputBucket(bucket)
	}

	db, err := sql.Open(drv.DriverName, conf.Stringify())
	if err != nil {
		return nil, fmt.Errorf("unable to connect to athena: %v", err)
	}

	client := &athenaDriver{
		c: builders.NewClient(db),
	}
	return client, nil
}

func (*Athena) GetHelpers(opts *core.TableOptions) map[string]string {
	return map[string]string{
		"List":    fmt.Sprintf(`SELECT * from "%s"."%s" LIMIT 500;`, opts.Schema, opts.Table),
		"Columns": fmt.Sprintf("DESCRIBE %s.%s;", opts.Schema, opts.Table),
		"Tables":  fmt.Sprintf("SHOW TABLES IN %s;", opts.Schema),
		"Views":   fmt.Sprintf("SHOW VIEWS IN %s;", opts.Schema),
	}
}
