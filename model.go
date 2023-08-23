package model

import (
	"fmt"

	"git.ultraware.nl/NiseVoid/qb"
	"git.ultraware.nl/NiseVoid/qb/qbdb"
	"git.ultraware.nl/NiseVoid/qb/qf"
)

type Model interface {
	GetTable() *qb.Table
	Select(f ...any) *qb.SelectBuilder
	Insert(f ...qb.Field) *qb.InsertBuilder
	Update() *qb.UpdateBuilder
	Delete(c1 qb.Condition, c2 ...qb.Condition) qb.Query
	F(f any) qb.Field
}

type model struct {
	table  qb.Table
	fields map[string]qb.TableField
}

type Option = func(*model)

func New(table string, options ...Option) Model {
	m := &model{
		table: qb.Table{Name: table},
	}

	for _, fn := range options {
		fn(m)
	}

	return m
}

func Columns(s ...string) func(*model) {
	return func(d *model) {
		for _, n := range s {
			d.fields[n] = qb.TableField{Parent: &d.table, Name: n}
		}
	}
}

func (d *model) GetTable() *qb.Table {
	return &d.table
}

func (d *model) Select(a ...any) *qb.SelectBuilder {
	var fields []qb.Field
	for _, v := range a {
		switch t := v.(type) {
		case []string:
			for _, s := range t {
				fields = append(fields, d.F(s))
			}
		default:
			fields = append(fields, d.F(v))
		}
	}

	return d.table.Select(fields)
}

func (d *model) Insert(f ...qb.Field) *qb.InsertBuilder {
	return d.table.Insert(f)
}

func (d *model) Update() *qb.UpdateBuilder {
	return d.table.Update()
}

func (d *model) Delete(c1 qb.Condition, c2 ...qb.Condition) qb.Query {
	return d.table.Delete(c1, c2...)
}

func (d *model) F(v any) qb.Field {
	switch f := v.(type) {
	case qf.CalculatedField:
		return f
	case qb.Field:
		return f
	case string:
		for k, v := range d.fields {
			if k == f {
				return &v
			}
		}
	}

	panic(fmt.Sprintf(`column %q does not exist on table %q`, v, d.table.Name))
}

func Scan(rows qbdb.Rows) []map[string]any {
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	cols = makeUnique(cols)

	var result []map[string]any

	for rows.Next() {
		// Create a slice of any's to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			panic(err)
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]any)
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			m[colName] = *val
		}

		result = append(result, m)
	}

	return result
}

func makeUnique(columns []string) []string {
	// this should be a driver responsibility...
	for i, col := range columns {
		if len(col) == 0 {
			columns[i] = fmt.Sprintf(`Column%d`, i)
		}

		is := indexOf(col, columns)
		if len(is) > 1 {
			for k, v := range is {
				if v == i {
					if k == 0 {
						continue
					}

					columns[i] = fmt.Sprintf(`%s_%d`, col, k)
				}
			}
		}
	}

	return columns
}

func indexOf(s string, in []string) []int {
	var is []int
	for i, v := range in {
		if s == v {
			is = append(is, i)
		}
	}

	return is
}
