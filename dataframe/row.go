package dataframe

import "errors"

type Row interface {
	GetField(position int) (interface{}, error)
	NumFields() int
	GetAllFields() []interface{}
}

type BasicRow struct {
	Fields []interface{}
}

func (b BasicRow) GetField(position int) (interface{}, error) {
	if len(b.Fields) < position {
		return nil, errors.New("Invalid Position of field")
	}
	return b.Fields[position-1], nil
}

func (b BasicRow) NumFields() int {
	return len(b.Fields)
}

func (b BasicRow) GetAllFields() []interface{}{
	return b.Fields
}

type Transformer interface {
	Transform(source Row) Row
}

