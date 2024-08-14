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
//	awsathena://[region][?options]
//
// Where:
//   - `region` is mandatory. It should match the intended
//     AWS Region to run queries on
//   - `options` is a & delimited list of key=value arguments.
//
// One of `work_group` or `s3_staging_dir` must be specified
//
// The supported `options` are:
//   - `work_group` define an Athena workgroup to run queries.
//   - `s3_staging_dir` S3 bucket and path where Athena stores results and metadata
//   - `read_only` enable read_only connection. default is true
//   - `moneywise` enable printing query cost to stdout. default is true
func (a *Athena) Connect(rawUrl string) (core.Driver, error) {
	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	conf := drv.NewNoOpsConfig()

	if u.Scheme != drv.DriverName {
		return nil, fmt.Errorf("unexpected scheme: %q", u.Scheme)
	}

	if u.Host != "" {
		conf.SetRegion(u.Host)
	} else {
		return nil, fmt.Errorf("missing region in url: %s", rawUrl)
	}

	params := u.Query()
	workgroup := params.Get("work_group")
	s3StagingDir := params.Get("s3_staging_dir")
	readOnly := params.Get("read_only")
	moneywise := params.Get("moneywise")

	if workgroup == "" && s3StagingDir == "" {
		return nil, fmt.Errorf("one of work_group or s3_staging_dir must be set in: %s", rawUrl)
	}
	if workgroup != "" {
		wg := drv.NewWG(workgroup, nil, nil)
		conf.SetWorkGroup(wg)
		conf.SetWGRemoteCreationAllowed(false)
	}
	if s3StagingDir != "" {
		conf.SetOutputBucket(s3StagingDir)
	}

	if readOnly == "false" {
		conf.SetReadOnly(false)
	}

	if moneywise == "false" {
		conf.SetMoneyWise(false)
	} else {
		conf.SetMoneyWise(true)
	}

	db, err := sql.Open(drv.DriverName, conf.Stringify())
	if err != nil {
		return nil, fmt.Errorf("unable to connect to athena: %v", err)
	}

	client := &athenaDriver{
		c:       builders.NewClient(db),
		columns: make(map[string][]*core.Column),
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
