package dstream

import (
	"math"
	"fmt"
)

// idcols: names of id column in each Dstream to join
// addcols: names of columns from ds2 to add to the left Dstream
//
func LeftJoin(ds1, ds2 Dstream, idcols, addcols []string) Dstream {
	var ret Dstream
	ret = ds1
	da := []Dstream{ds1, ds2}
	vm0 := make(map[string]int)
	vm1 := make(map[string]int)
	for ix, na := range da[0].Names() {
	    vm0[na] = ix
	}
	for ix, na := range da[1].Names() {
	    vm1[na] = ix
	}
	da[0].Reset()
	da[1].Reset()
	join := NewJoin(da, idcols)
	newdat := make([][]float64, len(addcols))
	var l0 int
	var rlength, ldiff int
	var rdat, temp []float64
	totrec := 0
	for join.Next() {
		l0 = len(da[0].GetPos(vm0[idcols[0]]).([]uint64)) // length of added columns for this chunk
		totrec+= l0
		if join.Status[1] {
			for j := 0; j < len(addcols); j++ {
				rdat = da[1].GetPos(vm1[addcols[j]]).([]float64)
				rlength = len(rdat)
				ldiff = l0 - rlength
				if ldiff > 0 {
					filler := rdat[len(rdat)-1]
					temp = make([]float64, l0)
					for i := 0; i < rlength; i++ {
						temp[i] = rdat[i]
					}
					for i := rlength; i < l0; i++ {
						temp[i] = filler
					}
					newdat[j] = append(newdat[j], temp...)
				} else if ldiff == 0 {
					newdat[j] = append(newdat[j], rdat...)
				} else {
					panic("number of rows in left Dstream smaller than number in right")
				}

			}
		} else { // no match on the id column	
			temp = make([]float64, l0)
			for k := 0; k < l0; k++ {
				temp[k] = math.NaN()
				fmt.Printf("writing nan\n")
			}
			for j := 0; j < len(addcols); j++ {
				newdat[j] = append(newdat[j], temp...)
			}
		}
	}

	for j, newcol := range newdat {
	       //fmt.Printf("\n---leftjoin length of column %v: %d\n", addcols[j], len(newcol))
		ret = Addcol(ret, newcol, addcols[j])
	}
	return ret
}
