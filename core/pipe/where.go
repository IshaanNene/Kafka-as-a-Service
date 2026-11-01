package pipe

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
)

// Filter is a CEL expression a record has to pass, as
//
//	value.amount > 100 && headers["region"] == "eu"
//
// It sees key and raw (the key and value as text), value (the value
// parsed as JSON, or its text when it isn't JSON), headers, topic,
// partition, offset and time.
type Filter struct {
	Expr string
	prg  cel.Program
}

// costLimit bounds one evaluation, so an expression can't spin on a
// record: comprehensions over a huge value stop with an error.
const costLimit = 1_000_000

var filterEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("key", cel.StringType),
		cel.Variable("raw", cel.StringType),
		cel.Variable("value", cel.DynType),
		cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("topic", cel.StringType),
		cel.Variable("partition", cel.IntType),
		cel.Variable("offset", cel.IntType),
		cel.Variable("time", cel.TimestampType),
	)
})

// NewFilter compiles expr, which has to be true or false.
func NewFilter(expr string) (*Filter, error) {
	env, err := filterEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("--where: %w", iss.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("--where: %q is a %s; want true or false", expr, t)
	}
	prg, err := env.Program(ast, cel.CostLimit(costLimit))
	if err != nil {
		return nil, fmt.Errorf("--where: %w", err)
	}
	return &Filter{Expr: expr, prg: prg}, nil
}

// Match is whether r passes. An expression that fails on r, say a field
// the value doesn't have, is an error; the pipe counts it as rejected.
func (f *Filter) Match(r Record) (bool, error) {
	out, _, err := f.prg.Eval(Vars(r))
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("--where gave %v, not true or false", out.Value())
	}
	return b, nil
}

// Vars are r as a filter sees it.
func Vars(r Record) map[string]any {
	hs := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		hs[h.Key] = h.Value
	}
	raw := string(r.Value)
	var value any = raw
	if t := strings.TrimSpace(raw); t != "" && (t[0] == '{' || t[0] == '[') && utf8.ValidString(t) {
		var v any
		if json.Unmarshal([]byte(t), &v) == nil {
			value = v
		}
	}
	return map[string]any{
		"key":       string(r.Key),
		"raw":       raw,
		"value":     value,
		"headers":   hs,
		"topic":     r.Topic,
		"partition": int64(r.Partition),
		"offset":    r.Offset,
		"time":      r.Time,
	}
}
