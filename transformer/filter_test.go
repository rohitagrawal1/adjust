package transformer

import (
	"github.com/adjust/dataframe"
	"reflect"
	"testing"
)

func TestWhereFilter(t *testing.T) {
	df := generateLeftDf()
	whereFilter := WhereFilter{
		Filter: Filter{
			func(row dataframe.Row) bool {
				field, _ := row.GetField(1)
				return field.(string) == "a"
			},
		},
	}
	dfFiltered, _ := whereFilter.Transform(df)

	expectedDfRows := make([]dataframe.Row, 0)
	rowsExpected1 := []interface{}{"a",2}
	expectedDfRows = append(expectedDfRows, dataframe.BasicRow{Fields: rowsExpected1})
	expectedDf := dataframe.Dataframe{
		Rows: expectedDfRows,
	}
	if !reflect.DeepEqual(expectedDf, dfFiltered){
		t.Fail()
	}
}
