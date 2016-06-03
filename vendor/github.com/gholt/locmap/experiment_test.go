package locmap

import "testing"

func BenchmarkInterfaceSliceAssignment(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]interface{}, 1024)
	e := &t{v: 1}
	for n := 0; n < b.N; n++ {
		d[n%1024] = e
	}
}

func BenchmarkStructSliceAssignment(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]*t, 1024)
	e := &t{v: 1}
	for n := 0; n < b.N; n++ {
		d[n%1024] = e
	}
}

func BenchmarkInterfaceSliceRetrieval(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]interface{}, 1024)
	for i := 0; i < 1024; i++ {
		d[i] = &t{v: i}
	}
	var e interface{}
	for n := 0; n < b.N; n++ {
		e = d[n%1024]
	}
	if e == nil {
		b.Fatal("e was nil")
	}
}

func BenchmarkStructSliceRetrieval(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]*t, 1024)
	for i := 0; i < 1024; i++ {
		d[i] = &t{v: i}
	}
	var e *t
	for n := 0; n < b.N; n++ {
		e = d[n%1024]
	}
	if e == nil {
		b.Fatal("e was nil")
	}
}

func BenchmarkInterfaceSliceRetrievalAndUsage(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]interface{}, 1024)
	for i := 0; i < 1024; i++ {
		d[i] = &t{v: i}
	}
	for n := 0; n < b.N; n++ {
		if d[n%1024].(*t).v != n%1024 {
			b.Fatalf("d[%d].v != %d was %d", n%1024, n%1024, d[n%1024].(*t).v)
		}
	}
}

func BenchmarkStructSliceRetrievalAndUsage(b *testing.B) {
	type t struct {
		v int
	}
	d := make([]*t, 1024)
	for i := 0; i < 1024; i++ {
		d[i] = &t{v: i}
	}
	for n := 0; n < b.N; n++ {
		if d[n%1024].v != n%1024 {
			b.Fatalf("d[%d].v != %d was %d", n%1024, n%1024, d[n%1024].v)
		}
	}
}
