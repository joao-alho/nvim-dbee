package adapters

import (
	"context"
	"errors"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver = (*athenaDriver)(nil)
)

type athenaDriver struct {
	c *builders.Client
}

func (c *athenaDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	// action := strings.ToLower(strings.Split(query, " ")[0])
	// if action == "update" || action == "delete" || action == "insert" || action == "merge" {
	// 	return c.c.Exec(ctx, query)
	// }
	return c.c.Query(ctx, query)
}

func (c *athenaDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	return c.c.ColumnsFromQuery(`
			SELECT column_name, data_type
			FROM information_schema.columns
			WHERE table_schema = '%s' and table_name = '%s';
			`, opts.Schema, opts.Table)
}

func (c *athenaDriver) Structure() ([]*core.Structure, error) {
	query := "SELECT table_schema, table_name, table_type FROM information_schema.tables;"

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	// this is compatible with Postgres, reuse some code
	return getStructure(rows)
}

func (c *athenaDriver) Close() {
	c.c.Close()
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

		schema, table, tableType := row[0].(string), row[1].(string), row[2].(string)

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
	case "BASE TABLE":
		return core.StructureTypeTable
	case "VIEW":
		return core.StructureTypeView
	default:
		return core.StructureTypeNone
	}
}
