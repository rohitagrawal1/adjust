package query

import (
	"fmt"
	df "github.com/adjust/dataframe"
	tf "github.com/adjust/transformer"
)

type Query interface {
	Execute() (df.Dataframe, error)
}

type AggregateQuery struct {
	Dataframe df.Dataframe
	Grouper   Grouper
	Reducer   Reducer
}

func (a AggregateQuery) Execute() (df.Dataframe, error) {
	groupedRows := a.Grouper.Group(a.Dataframe.Rows)
	reducedRows := make([]df.Row, 0)
	for _, group := range groupedRows {
		rr := a.Reducer.reduce(group)
		reducedRows = append(reducedRows, rr)
	}
	return df.Dataframe{reducedRows}, nil
}

type Reducer interface {
	reduce([]df.Row) df.Row
}

type Counter struct {
	ReducingOnColumn int
}

func (c Counter) reduce(rows []df.Row) df.Row {
	reduced := make([]interface{}, 0)
	if f, err := rows[0].GetField(c.ReducingOnColumn); err == nil{
		reduced = append(reduced, f)
		reduced = append(reduced, len(rows))
	}
	return df.BasicRow{
		reduced,
	}
}



type Grouper interface {
	Group(row []df.Row) [][]df.Row
}

type SameValueGrouper struct {
	FieldToGroupOn int
}

func (g SameValueGrouper) Group(rows []df.Row) [][]df.Row {
	m := make(map[interface{}][]df.Row)
	for _, row := range rows {
		key, err := row.GetField(g.FieldToGroupOn)
		strKey := fmt.Sprintf("%v", key)
		if err == nil && key != nil{
			if gp, ok1 := m[strKey]; ok1 {
				m[strKey] = append(gp, row)
			}else{
				r := make([]df.Row, 1)
				r[0] = row
				m[strKey] = r
			}
		}else{
			fmt.Println("Key is nil")
		}
	}
	var res [][]df.Row
	for _, v := range m {
		res = append(res, v)
	}
	return res
}

/**
	Returns top 10 most frequent element in the column at position fieldToAggregate,
	after an equals filter on the column at position whereConditionFieldPos with values contained in wherePassingValues map
 */
func Top10BasedOnColumnFreq(dfToProcess df.Dataframe, fieldToAggregate int, wherePassingValues map[string]bool, whereConditionFieldPos int) (*df.Dataframe, error) {
	grouper := SameValueGrouper{
		fieldToAggregate,
	}
	reducer := Counter{
		fieldToAggregate,
	}
	var query Query
	filterFunc := func (row df.Row) bool {
		event, _ := row.GetField(whereConditionFieldPos)
		if ok, _ := wherePassingValues[event.(string)];ok{
			return true
		}
		return false
	}
	f := tf.WhereFilter{
		Filter: tf.Filter{
			Filter: filterFunc,
		},
	}
	df, err := f.Transform(dfToProcess)
	if err != nil{
		return nil, err
	}
	if &df != nil{
		query = AggregateQuery{
			Dataframe: df,
			Grouper: grouper,
			Reducer: reducer,
		}
	}
	df1 , err := query.Execute()
	if err != nil{
		return nil, err
	}

	kFilter := tf.TopKFilter{
		Column: 2,
		K:      10,
	}
	df2, err := kFilter.Transform(df1)

	if err != nil{
		return nil, err
	}
	return df2, nil


}
