package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestXCosineSim(t *testing.T) {
	const float64EqualityThreshold = 1e-9
	ctx := createContext(t)
	fc := funcs[ast.XCosineSim]
	tbl := []struct {
		Input    []interface{}
		Expected float64
	}{
		{[]interface{}{`[1.1,1.2,1.3,1.4,1.5]`, `[1.1,1.2,1.3,1.4,1.5]`}, 1.0},
		{[]interface{}{`[2.1,2.2,2.3,2.4,2.5]`, `[1.1,1.2,1.3,1.4,1.5]`}, 0.9988980834329954},
		{[]interface{}{`[-1.0,-2.0,-3.0,-4.0,-5.0,-6.0]`, `[1.0,1.1,1.2,1.3,1.4,1.5]`}, -0.949807619836754},
	}
	dtbl := tblToDtbl(tbl)
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		var result = d.GetFloat64()
		var expected = tt["Expected"][0].GetFloat64()
		var approximatelyEqual = math.Abs(result-expected) <= float64EqualityThreshold
		require.Equal(t, true, approximatelyEqual)
	}
}

func TestDotProduct(t *testing.T) {
	const float64EqualityThreshold = 1e-9
	ctx := createContext(t)
	fc := funcs[ast.XDotProduct]
	tbl := []struct {
		Input    []interface{}
		Expected float64
	}{
		{[]interface{}{`[1.1,1.2,1.3,1.4,1.5]`, `[1.1,1.2,1.3,1.4,1.5]`}, 8.55},
		{[]interface{}{`[2.1,2.2,2.3,2.4,2.5]`, `[1.1,1.2,1.3,1.4,1.5]`}, 15.05},
		{[]interface{}{`[-1.0,-2.0,-3.0,-4.0,-5.0,-6.0]`, `[1.0,1.1,1.2,1.3,1.4,1.5]`}, -28.0},
	}
	dtbl := tblToDtbl(tbl)
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		var result = d.GetFloat64()
		var expected = tt["Expected"][0].GetFloat64()
		var approximatelyEqual = math.Abs(result-expected) <= float64EqualityThreshold
		require.Equal(t, true, approximatelyEqual)
	}
}
