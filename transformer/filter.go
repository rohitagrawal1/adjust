package transformer

import (
	"container/heap"
	"errors"
	"strconv"
	df "github.com/adjust/dataframe"
)

type Filter struct{
	Filter func(df.Row) bool
}

type WhereFilter struct {
	Filter Filter
}

func (w WhereFilter) Transform(source df.Dataframe) (df.Dataframe, error){
	filteredRows := make([]df.Row, 0)
	for _, row:= range source.Rows {
		if w.Filter.Filter(row) {
			filteredRows = append(filteredRows, row)
		}
	}
	return df.Dataframe{filteredRows}, nil
}


type TopKFilter struct{
	Column int
	K      int
}

func (t TopKFilter)Transform(source df.Dataframe) (*df.Dataframe, error) {
	h := &rowHeap{
		Elements: make([]*df.Row,0),
		FieldPosition: t.Column,
	}
	for i :=0;i<t.K;i++{
		heap.Push(h, source.Rows[i])
	}
	for i := t.K;i< len(source.Rows);i++{
		el, err := (*h.Elements[0]).GetField(t.Column);
		if err != nil{
			return nil, errors.New("transformation error")
		}
		switch v := el.(type){
		case string:
			stringField , _ := source.Rows[len(source.Rows)-1].GetField(t.Column)
			peekElem, err := strconv.Atoi(stringField.(string))
			if err != nil{
				return nil, errors.New("cannot convert string to int")
			}
			elInt, err := strconv.Atoi(v)
			if err != nil{
				return nil, errors.New("cannot convert string to int")
			}
			if elInt > peekElem{
				heap.Push(h, source.Rows[i])
			}
		case int:
			fieldInt, err := source.Rows[i].GetField(t.Column)
			if err != nil{
				return nil, errors.New("could not get field, column count mismatch")
			}
			if fieldInt.(int) > v{
				heap.Pop(h)
				heap.Push(h, source.Rows[i])
			}
		}

		//		if((*h.Elements[len(h.Elements)-1]).GetField(t.Column) < )
	}
	res := make([]df.Row, len(h.Elements))
	for i:=len(h.Elements)-1;i>=0;i-- {
		popped := heap.Pop(h)
		res[i] =  *(popped.(*df.Row))

	}
	return &df.Dataframe{Rows: res}, nil
}

type rowHeap struct{
	Elements      []*df.Row
	FieldPosition int
}

func (h rowHeap) Len() int {
	return len(h.Elements)
}
func (h rowHeap) Less(i, j int) bool {
	iElem, _ := (*h.Elements[i]).GetField(h.FieldPosition)
	jElem, _ := (*h.Elements[j]).GetField(h.FieldPosition)

	switch (iElem).(type){
	case string:
		iElemInt, _ := strconv.Atoi(iElem.(string))
		jElemInt, _ := strconv.Atoi(jElem.(string))
		return iElemInt < jElemInt
	case int:
		//fmt.Println("here.")
		return iElem.(int) < jElem.(int)
	default:
		return iElem.(string) < jElem.(string)
	}
}

func (h rowHeap) Swap(i, j int) {
	h.Elements[i], h.Elements[j] = h.Elements[j], h.Elements[i]
}

func (h *rowHeap) Push(x interface{}) {
	e := x.(df.Row)
	h.Elements = append(h.Elements, &e)
}

func (h *rowHeap) Pop() interface{} {
	old := h.Elements
	n := len(old)
	x := old[n-1]
	h.Elements = old[0:n-1]
	return x
}