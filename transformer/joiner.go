package transformer

import "github.com/adjust/dataframe"

/**
	Joins left and right Df on column JoinColumnLeft in leftDF and JoinColumnRight in rightDF, It assumes that the right DF has the joining Column as a primary key.
 */
type Joiner struct{
	LeftDF          dataframe.Dataframe
	RightDF         dataframe.Dataframe
	JoinColumnLeft  int
	JoinColumnRight int
}

type DFTransformer interface {
	Transform(left dataframe.Dataframe, right dataframe.Dataframe) (dataframe.Dataframe, error)
}

func (j Joiner)Transform() (dataframe.Dataframe, error){
	joinedRows := make([]dataframe.Row,0)
	rightMap := make(map[interface{}]dataframe.Row)
	for _, rightRow := range j.RightDF.Rows{
		if rightJoinColValue, err := rightRow.GetField(j.JoinColumnRight); err == nil{
			rightMap[rightJoinColValue] = rightRow
		}
	}
	for _, leftRow := range j.LeftDF.Rows {
		if joinColLeft, err := leftRow.GetField(j.JoinColumnLeft); err == nil{
			if v, ok := rightMap[joinColLeft]; ok{
				joinedRows = append(joinedRows, dataframe.BasicRow{Fields: append(leftRow.GetAllFields(), v.GetAllFields()...)})
			}
		}
	}
	return dataframe.Dataframe{
		Rows: joinedRows,
	}, nil
}