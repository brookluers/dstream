// GENERATED CODE, DO NOT EDIT

package dstream

import (
	"fmt"
	"os"
)

// EqualReport compares two Dstream values.  If they are not equal,
// further information is written to the standard error stream.  Equality
// here implies that the data values, types, order, and chunk
// boundaries are all identical.
func EqualReport(x, y Dstream, report bool) bool {

	x.Reset()
	y.Reset()

	// Check variable names
	if !aequalstring(x.Names(), y.Names()) {
		if report {
			msg := fmt.Sprintf("Names are not equal:\nx: %v\ny: %v\n",
				x.Names(), y.Names())
			os.Stderr.WriteString(msg)
		}
		return false
	}

	if x.NumVar() != y.NumVar() {
		if report {
			msg := fmt.Sprintf("Number of variables are not equal:\nx: %d\ny: %d\n",
				x.NumVar(), y.NumVar())
			os.Stderr.WriteString(msg)
		}
		return false
	}

	for chunk := 0; x.Next(); chunk++ {
		if !y.Next() {
			if report {
				msg := fmt.Sprintf("unequal numbers of chunks (y has fewer chunks than x)\n")
				print(msg)
			}
			return false
		}
		for j := 0; j < x.NumVar(); j++ {
			switch v := x.GetPos(j).(type) {

			case []string:
				u, ok := y.GetPos(j).([]string)
				if !ok || !aequalstring(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []float64:
				u, ok := y.GetPos(j).([]float64)
				if !ok || !aequalfloat64(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []float32:
				u, ok := y.GetPos(j).([]float32)
				if !ok || !aequalfloat32(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []uint64:
				u, ok := y.GetPos(j).([]uint64)
				if !ok || !aequaluint64(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []uint32:
				u, ok := y.GetPos(j).([]uint32)
				if !ok || !aequaluint32(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []uint16:
				u, ok := y.GetPos(j).([]uint16)
				if !ok || !aequaluint16(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []uint8:
				u, ok := y.GetPos(j).([]uint8)
				if !ok || !aequaluint8(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []int64:
				u, ok := y.GetPos(j).([]int64)
				if !ok || !aequalint64(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []int32:
				u, ok := y.GetPos(j).([]int32)
				if !ok || !aequalint32(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []int16:
				u, ok := y.GetPos(j).([]int16)
				if !ok || !aequalint16(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []int8:
				u, ok := y.GetPos(j).([]int8)
				if !ok || !aequalint8(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			case []int:
				u, ok := y.GetPos(j).([]int)
				if !ok || !aequalint(v, u) {
					if report {
						fmt.Printf("Chunk %d, %s\n", chunk, x.Names()[j])
						fmt.Printf("  Unequal floats:\n    (1) %v\n    (2) %v\n", v, u)
					}
					return false
				}

			default:
				if report {
					print("mismatched types")
				}
				return false
			}
		}
	}

	if y.Next() {
		if report {
			msg := fmt.Sprintf("unequal numbers of chunks (x has fewer chunks than y)\n")
			print(msg)
		}
		return false
	}

	return true
}

func aequalstring(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalfloat64(x, y []float64) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalfloat32(x, y []float32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequaluint64(x, y []uint64) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequaluint32(x, y []uint32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequaluint16(x, y []uint16) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequaluint8(x, y []uint8) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalint64(x, y []int64) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalint32(x, y []int32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalint16(x, y []int16) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalint8(x, y []int8) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}

func aequalint(x, y []int) bool {
	if len(x) != len(y) {
		return false
	}
	for i, v := range x {
		if v != y[i] {
			return false
		}
	}
	return true
}
