package adapters

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver = (*athenaDriver)(nil)
)

type athenaDriver struct {
	c         *builders.Client
	columns   map[string][]*core.Column
	structure []*core.Structure
}

func (c *athenaDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	return c.c.Query(ctx, query)
}

func (c *athenaDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	db := strings.Trim(opts.Schema, `"`)
	return getColumnsFromGlue(db, opts.Table)
}

func (c *athenaDriver) Structure() ([]*core.Structure, error) {
	//
	if c.structure != nil {
		return c.structure, nil
	}

	// this is compatible with Postgres, reuse some code
	// structure, err := getStructure(rows)
	structure, cols, err := getStructureFromGlue(context.TODO())
	if err != nil {
		return nil, err
	}
	c.structure = structure
	c.columns = cols

	return structure, nil
}

func (c *athenaDriver) Close() {
	c.c.Close()
}

func getStructureFromGlue(ctx context.Context) ([]*core.Structure, map[string][]*core.Column, error) {
	children := make(map[string][]*core.Structure)
	cols := make(map[string][]*core.Column)
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, err
	}
	client := glue.NewFromConfig(config)

	params := &glue.GetDatabasesInput{
		AttributesToGet: []types.DatabaseAttributes{types.DatabaseAttributesName},
	}

	resp, err := client.GetDatabases(ctx, params)
	if err != nil {
		return nil, nil, err
	}

	for _, db := range resp.DatabaseList {
		params := &glue.GetTablesInput{
			DatabaseName: db.Name,
			// AttributesToGet: []types.TableAttributes{types.TableAttributesName, types.TableAttributesTableType},
		}
		resp, err := client.GetTables(ctx, params)
		if err != nil {
			return nil, nil, err
		}
		i := 1
		var formatted_db_name string
		_, err = fmt.Sscanf(*db.Name, "%d", &i)
		if err == nil {
			formatted_db_name = fmt.Sprintf(`"%s"`, *db.Name)
		} else {
			formatted_db_name = *db.Name
		}
		for _, tbl := range resp.TableList {
			key := fmt.Sprintf(`%s%s`, *tbl.DatabaseName, formatted_db_name)
			children[formatted_db_name] = append(children[formatted_db_name], &core.Structure{
				Name:   *tbl.Name,
				Schema: formatted_db_name,
				Type:   getStructureType(*tbl.TableType),
			})
			if tbl.StorageDescriptor != nil {
				for _, col := range tbl.StorageDescriptor.Columns {
					cols[key] = append(cols[key], &core.Column{
						Name: *col.Name,
						Type: *col.Type,
					})
				}
			}
		}
	}

	var structure []*core.Structure
	for k, v := range children {
		structure = append(structure, &core.Structure{
			Name:     k,
			Schema:   k,
			Type:     core.StructureTypeNone,
			Children: v,
		})
	}

	return structure, cols, nil
}

func getColumnsFromGlue(db string, table string) ([]*core.Column, error) {
	var columns []*core.Column
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	client := glue.NewFromConfig(config)
	params := &glue.GetTableInput{
		Name:         &table,
		DatabaseName: &db,
	}
	tbl, err := client.GetTable(context.TODO(), params)
	for _, col := range tbl.Table.StorageDescriptor.Columns {
		columns = append(columns, &core.Column{
			Name: *col.Name,
			Type: *col.Type,
		})
	}

	return columns, nil

}

// lifted straight from the postgres adapter
func getStructure(rows core.ResultStream) ([]*core.Structure, error) {
	children := make(map[string][]*core.Structure)

	for rows.HasNext() {
		row, err := rows.Next()
		if err != nil {
			return nil, err
		}
		if len(row) < 3 {
			return nil, errors.New("could not retrieve structure: insufficient info")
		}

		var schema string
		i := 0
		_, err = fmt.Sscanf(row[0].(string), "%d", &i)
		if err != nil {
			schema = row[0].(string)
		} else {
			schema = fmt.Sprintf(`"%s"`, row[0].(string))
		}

		table, tableType := row[1].(string), row[2].(string)

		children[schema] = append(children[schema], &core.Structure{
			Name:   table,
			Schema: schema,
			Type:   getStructureType(tableType),
		})
	}

	var structure []*core.Structure

	for k, v := range children {
		structure = append(structure, &core.Structure{
			Name:     k,
			Schema:   k,
			Type:     core.StructureTypeNone,
			Children: v,
		})
	}

	return structure, nil
}

// lifted straight from the postgres adapter
func getStructureType(typ string) core.StructureType {
	switch typ {
	case "EXTERNAL_TABLE":
		return core.StructureTypeTable
	case "VIRTUAL_VIEW":
		return core.StructureTypeView
	default:
		return core.StructureTypeNone
	}
}
