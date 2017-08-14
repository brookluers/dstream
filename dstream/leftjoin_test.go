package dstream

import (	
//       "fmt"
	"testing"
	"bytes"
	"math"
//	"os"
//	"io"
)

func TestLeftJoin(t *testing.T) {

data1 := `id,v1,v2,v3
1,2,3,4
1,3,4,5
2,4,5,6
3,5,6,7
3,99,99,99
3,100,101,102
4,200,201,202
`

data2 := `id,z1,z2
1,-0.5,-0.75
2,-0.9,-0.8
3,-0.3,-0.4
`
	b1 := bytes.NewReader([]byte(data1))
	d1 := FromCSV(b1).SetFloatVars([]string{"id","v1","v2","v3"}).HasHeader().Done()
	b2 := bytes.NewReader([]byte(data2))
	d2 := FromCSV(b2).SetFloatVars([]string{"id","z1","z2"}).HasHeader().Done()
	d1 = Convert(d1, "id", "uint64")
	d2 = Convert(d2, "id", "uint64")
	d1 = Segment(d1, []string{"id"})
	d2 = Segment(d2, []string{"id"})

	d1.Reset()
	d2.Reset()
	d1 = LeftJoin(d1, d2, []string{"id","id"}, []string{"z2"})
	d1.Reset()
	t.Logf("---after joining d1 and d2-----")
	t.Logf("len(d1.Names()) = %v\n", len(d1.Names()))
	if len(d1.Names()) != 5 {
	   t.Fail()
	}
	d1z2 := GetCol(d1, "z2").([]float64)
	ans := []float64{-0.75, -0.75, -0.8, -0.4, -0.4, -0.4, math.NaN()}
	for i := 0; i < len(ans); i++{
	    t.Logf("d1z2[%d] = %v\n", i, d1z2[i])
	    t.Logf("ans[%d] = %v\n", i, ans[i])
	    if math.IsNaN(d1z2[i]) {
	       if !math.IsNaN(ans[i]) {
	       	  t.Fail()
	       }
	    } else{
	      if d1z2[i] != ans[i] {
	      	 t.Fail()
	      }
	    }
	}
}
