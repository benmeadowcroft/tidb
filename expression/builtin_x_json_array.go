package expression

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ functionClass = &xcosinesimFunctionClass{}
	_ functionClass = &xdotproductFunctionClass{}
)

var (
	_ builtinFunc = &builtinXcosinesimSig{}
)

type xcosinesimFunctionClass struct {
	baseFunctionClass
}

func (c *xcosinesimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, 2)
	argTps = append(argTps, types.ETJson)
	argTps = append(argTps, types.ETJson)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinXcosinesimSig{bf}
	return sig, nil
}

type builtinXcosinesimSig struct {
	baseBuiltinFunc
}

func (b *builtinXcosinesimSig) Clone() builtinFunc {
	newSig := &builtinXcosinesimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinXcosinesimSig) evalReal(row chunk.Row) (float64, bool, error) {
	const zero float64 = 0.0

	arr1, isNull, err := ExtractFloat64Array(b.ctx, b.args[0], row)
	if isNull || err != nil {
		return zero, isNull, err
	}

	arr2, isNull, err := ExtractFloat64Array(b.ctx, b.args[1], row)
	if isNull || err != nil {
		return zero, isNull, err
	}

	cosineSimilarity, cosErr := Cosine(arr1, arr2)

	if cosErr != nil {
		cosErr = errors.Wrap(cosErr, "Invalid JSON Array: an array of non-zero numbers was expected")
		return zero, false, cosErr
	}
	return cosineSimilarity, false, cosErr
}

type xdotproductFunctionClass struct {
	baseFunctionClass
}

func (c *xdotproductFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, 2)
	argTps = append(argTps, types.ETJson)
	argTps = append(argTps, types.ETJson)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinXdotproductSig{bf}
	return sig, nil
}

type builtinXdotproductSig struct {
	baseBuiltinFunc
}

func (b *builtinXdotproductSig) Clone() builtinFunc {
	newSig := &builtinXdotproductSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinXdotproductSig) evalReal(row chunk.Row) (float64, bool, error) {
	const zero float64 = 0.0

	arr1, isNull, err := ExtractFloat64Array(b.ctx, b.args[0], row)
	if isNull || err != nil {
		return zero, isNull, err
	}

	arr2, isNull, err := ExtractFloat64Array(b.ctx, b.args[1], row)
	if isNull || err != nil {
		return zero, isNull, err
	}

	dotProduct, cosErr := DotProduct(arr1, arr2)

	if cosErr != nil {
		cosErr = errors.Wrap(cosErr, "Invalid JSON Array: an array of non-zero numbers were expected")
		return zero, false, cosErr
	}
	return dotProduct, false, cosErr
}

func ExtractFloat64Array(ctx sessionctx.Context, expr Expression, row chunk.Row) (values []float64, isNull bool, err error) {
	json1, isNull, err := expr.EvalJSON(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	values, err = AsFloat64Array(json1)
	if err != nil {
		return nil, false, err
	}
	return values, false, nil
}

func AsFloat64Array(binJson types.BinaryJSON) (values []float64, err error) {
	if binJson.TypeCode != types.JSONTypeCodeArray {
		err = errors.New("Invalid JSON Array: an array of numbers were expected")
		return nil, err
	}

	var arrCount int = binJson.GetElemCount()
	values = make([]float64, arrCount)
	for i := 0; i < arrCount && err == nil; i++ {
		var elem = binJson.ArrayGetElem(i)
		values[i], err = types.ConvertJSONToFloat(fakeSctx, elem)
	}
	return values, err
}

func DotProduct(a []float64, b []float64) (cosine float64, err error) {
	if len(a) != len(b) {
		return 0.0, errors.New("Invalid vectors: two arrays of the same length were expected")
	}
	if len(a) == 0 {
		return 0.0, errors.New("Invalid vectors: two non-zero length arrays were expected")
	}

	sum := 0.0

	for i := range a {
		sum += a[i] * b[i]
	}
	return sum, nil
}

func Cosine(a []float64, b []float64) (cosine float64, err error) {
	if len(a) != len(b) {
		return 0.0, errors.New("Invalid vectors: two arrays of the same length were expected")
	}
	if len(a) == 0 {
		return 0.0, errors.New("Invalid vectors: two non-zero length arrays were expected")
	}

	sum := 0.0
	s1 := 0.0
	s2 := 0.0

	for i := range a {
		sum += a[i] * b[i]
		s1 += a[i] * a[i]
		s2 += b[i] * b[i]
	}
	return sum / (math.Sqrt(s1) * math.Sqrt(s2)), nil
}
