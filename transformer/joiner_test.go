package transformer

import (
	"github.com/adjust/dataframe"
	"reflect"
	"testing"
)

func TestJoiner(t *testing.T) {
	joiner := Joiner{
		generateLeftDf(),
		generateRightDf(),
		2,
		2,
	}
	dfActual, _ :=  joiner.Transform()

	expectedDfRows := make([]dataframe.Row, 0)
	rowsExpected1 := []interface{}{"a",2, 1, 2}
	rowsExpected2 := []interface{}{"b",2, 1, 2}
	rowsExpected3 := []interface{}{"c",3, 1, 3}
	expectedDfRows = append(expectedDfRows, dataframe.BasicRow{Fields: rowsExpected1})
	expectedDfRows = append(expectedDfRows, dataframe.BasicRow{Fields: rowsExpected2})
	expectedDfRows = append(expectedDfRows, dataframe.BasicRow{Fields: rowsExpected3})

	expectedDf := dataframe.Dataframe{
		Rows: expectedDfRows,
	}

	if !reflect.DeepEqual(expectedDf, dfActual){
		t.Fail()
	}
}

func generateLeftDf() dataframe.Dataframe {
	leftDfRows := make([]dataframe.Row, 0)
	rowsLeft1 := []interface{}{"a",2}
	rowsLeft2 := []interface{}{"b",2}
	rowsLeft3 := []interface{}{"c",3}
	leftDfRows = append(leftDfRows, dataframe.BasicRow{Fields: rowsLeft1})
	leftDfRows = append(leftDfRows, dataframe.BasicRow{Fields: rowsLeft2})
	leftDfRows = append(leftDfRows, dataframe.BasicRow{Fields: rowsLeft3})
	leftDf := dataframe.Dataframe{
		Rows: leftDfRows,
	}
	return leftDf
}

func generateRightDf() dataframe.Dataframe {
	rightDfRows := make([]dataframe.Row, 0)
	rowsRight1 := []interface{}{1,3}
	rowsRight2 := []interface{}{1,2}
	rowsRight3 := []interface{}{5,1}

	rightDfRows = append(rightDfRows, dataframe.BasicRow{Fields: rowsRight1})
	rightDfRows = append(rightDfRows, dataframe.BasicRow{Fields: rowsRight2})
	rightDfRows = append(rightDfRows, dataframe.BasicRow{Fields: rowsRight3})
	rightDf := dataframe.Dataframe{
		Rows: rightDfRows,
	}
	return rightDf
}