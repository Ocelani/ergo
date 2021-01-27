package etf

import (
	"bytes"
	"reflect"
	"testing"
)

func TestTermIntoStruct_Slice(t *testing.T) {
	dest := struct{ Slice []byte }{}

	tests := []struct {
		want []byte
		term Term
	}{
		{[]byte{1, 2, 3}, List{1, 2, 3}},
		{[]byte{3, 4, 5}, Tuple{3, 4, 5}},
	}

	for _, tt := range tests {
		term := Map{"slice": tt.term}
		if err := TermIntoStruct(term, &dest); err != nil {
			t.Errorf("%#v: conversion failed: %v", term, err)
		}

		if !bytes.Equal(dest.Slice, tt.want) {
			t.Errorf("%#v: got %v, want %v", term, dest.Slice, tt.want)
		}
	}
	tests1 := []struct {
		want [][]float32
		term Term
	}{
		{[][]float32{[]float32{1.23, 2.34, 3.45}, []float32{4.56, 5.67, 6.78}, []float32{7.89, 8.91, 9.12}}, List{List{1.23, 2.34, 3.45}, List{4.56, 5.67, 6.78}, List{7.89, 8.91, 9.12}}},
		{[][]float32{[]float32{10.23, 20.34, 30.45}, []float32{40.56, 50.67, 60.78}, []float32{70.89, 80.91, 90.12}}, Tuple{Tuple{10.23, 20.34, 30.45}, Tuple{40.56, 50.67, 60.78}, Tuple{70.89, 80.91, 90.12}}},
	}
	dest1 := struct{ Slice [][]float32 }{}

	for _, tt := range tests1 {
		term := Map{"slice": tt.term}
		if err := TermIntoStruct(term, &dest1); err != nil {
			t.Errorf("%#v: conversion failed: %v", term, err)
		}

		if !reflect.DeepEqual(dest1.Slice, tt.want) {
			t.Errorf("%#v: got %v, want %v", term, dest1.Slice, tt.want)
		}
	}
}

func TestTermIntoStruct_Array(t *testing.T) {
	dest := struct{ Array [3]byte }{}

	tests := []struct {
		want [3]byte
		term Term
	}{
		{[...]byte{1, 2, 3}, List{1, 2, 3}},
		{[...]byte{3, 4, 5}, Tuple{3, 4, 5}},
	}

	for _, tt := range tests {
		term := Map{"array": tt.term}
		if err := TermIntoStruct(term, &dest); err != nil {
			t.Errorf("%#v: conversion failed: %v", term, err)
		}

		if dest.Array != tt.want {
			t.Errorf("%#v: got %v, want %v", term, dest.Array, tt.want)
		}
	}

	tests1 := []struct {
		want [3][3]float64
		term Term
	}{
		{[3][3]float64{[...]float64{1.23, 2.34, 3.45}, [...]float64{4.56, 5.67, 6.78}, [...]float64{7.89, 8.91, 9.12}}, List{List{1.23, 2.34, 3.45}, List{4.56, 5.67, 6.78}, List{7.89, 8.91, 9.12}}},
		{[3][3]float64{[...]float64{10.23, 20.34, 30.45}, [...]float64{40.56, 50.67, 60.78}, [...]float64{70.89, 80.91, 90.12}}, Tuple{Tuple{10.23, 20.34, 30.45}, Tuple{40.56, 50.67, 60.78}, Tuple{70.89, 80.91, 90.12}}},
	}
	dest1 := struct{ Array [3][3]float64 }{}

	for _, tt := range tests1 {
		term := Map{"array": tt.term}
		if err := TermIntoStruct(term, &dest1); err != nil {
			t.Errorf("%#v: conversion failed: %v", term, err)
		}

		if !reflect.DeepEqual(dest1.Array, tt.want) {
			t.Errorf("%#v: got %v, want %v", term, dest1.Array, tt.want)
		}
	}
}

func TestTermIntoStruct_Struct(t *testing.T) {
	type testAA struct {
		A []bool
		B uint32
		C string
	}

	type testStruct struct {
		AA testAA
		BB float64
		CC *testStruct
	}
	type testItem struct {
		Want testStruct
		Term Term
	}

	dest := testStruct{}
	tests := []testItem{
		testItem{
			Want: testStruct{
				AA: testAA{
					A: []bool{true, false, false, true, false},
					B: 8765,
					C: "test value",
				},
				BB: 3.13,
				CC: &testStruct{
					BB: 4.14,
					CC: &testStruct{
						AA: testAA{
							A: []bool{false, true},
							B: 5,
						},
					},
				},
			},
			Term: Tuple{ //testStruct
				Tuple{ // AA testAA
					List{true, false, false, true, false}, // A []bool
					8765,                                  // B uint32
					"test value",                          // C string
				},
				3.13, // BB float64
				Tuple{ // CC *testStruct
					Tuple{}, // AA testAA (empty)
					4.14,    // BB float64
					Tuple{ // CC *testStruct
						Tuple{ // AA testAA
							List{false, true}, // A []bool
							5,                 // B uint32
							// C string (empty)
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		if err := TermIntoStruct(tt.Term, &dest); err != nil {
			t.Errorf("%#v: conversion failed %v", tt.Term, err)
		}

		if !reflect.DeepEqual(dest, tt.Want) {
			t.Errorf("%#v: got %#v, want %#v", tt.Term, dest, tt.Want)
		}
	}
}
