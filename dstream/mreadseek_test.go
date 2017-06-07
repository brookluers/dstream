package dstream

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/gonum/matrix/mat64"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func expit(i, j int, v float64) float64 {
	return 1 / (1 + math.Exp(-v))
}

//gendat generates n bivariate normal pairs (x1, x2)
// and a binary response Y
// Pr(Y=1 | x1, x2) = expit(beta[0]*x1 + beta[1]*x2)
// n observations per file, and nfiles files created
func gendat(n, nfiles int) []string {

	rand.Seed(42)
	p := 2
	cor1 := mat64.NewSymDense(p, []float64{1, 0.6, 0.6, 1})
	cor2 := mat64.NewSymDense(p, []float64{1, -0.8, -0.8, 1})
	var chol1, chol2 mat64.Cholesky
	if ok := chol1.Factorize(cor1); !ok {
		fmt.Println("matrix is not positive semi-definite")
	}
	if ok := chol2.Factorize(cor2); !ok {
		fmt.Println("matrix is not positive semi-definite")
	}

	var l1, l2 mat64.TriDense
	l1.LFromCholesky(&chol1)
	l2.LFromCholesky(&chol2)
	var x1, x2, x, logits mat64.Dense
	var probs mat64.Dense
	beta := mat64.NewVector(p, []float64{-0.8, 0.4})
	z := mat64.NewDense(p, n, nil) // new slice allocated for backing slice

	files := make([]*os.File, nfiles)
	fnames := make([]string, nfiles)
	defer closeFiles(files)
	var err error
	var w *csv.Writer
	row := make([]string, p+3)
	for j := 0; j < nfiles; j++ {
		fnames[j] = "dat" + strconv.Itoa(j) + ".txt"
		files[j], err = os.Create(fnames[j])
		if err != nil {
			panic(err)
		}
		for i := 0; i < n*p; i++ {
			z.Set(i%p, i/p, rand.NormFloat64()) // z is p rows by n columns
		}
		x1.Mul(&l1, z)
		x2.Mul(&l2, z)
		x.Augment(x1.Slice(0, p, 0, n/2), x2.Slice(0, p, n/2, n)) // x is an n by p matrix
		logits.Mul(beta.T(), &x)
		probs.Apply(expit, &logits) // Pr(Y=1 | x1, x2)

		w = csv.NewWriter(files[j])
		w.Write([]string{"x1", "x2", "prob", "y", "id"})
		w.Flush()

		for i := 0; i < n; i++ {
			for k := 0; k < p; k++ {
				row[k] = fmt.Sprintf("%f", x.At(k, i))
			}
			row[p] = fmt.Sprintf("%f", probs.At(0, i))
			if r := rand.Float64(); r < probs.At(0, i) {
				row[p+1] = fmt.Sprintf("%d", 1)
			} else {
				row[p+1] = fmt.Sprintf("%d", 0)
			}
			row[p+2] = strconv.Itoa(j)
			if err = w.Write(row); err != nil {
				panic(err)
			}

		}
		w.Flush()
		if err = w.Error(); err != nil {
			panic(err)
		}
	}
	return fnames

}

func closeFiles(files []*os.File) error {
	for _, f := range files {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func rmFiles(fnames []string) error {
	for _, nm := range fnames {
		err := os.Remove(nm)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestMultiReadSeek0(t *testing.T) {
	nfiles := 4
	n := 7
	csize := 10
	fnames := gendat(n, nfiles) // n observations in each of nfiles files
	fmt.Printf("Made %d files, %d observations per file, initial chunk size %d\n", nfiles, n, csize)

	//Print all the generated data files
	for j := 0; j < len(fnames); j++ {
		fcontents, ferr := ioutil.ReadFile(fnames[j])
		if ferr == nil {
			fmt.Printf("-----------File %v-----------\n", fnames[j])
			fmt.Println(string(fcontents))
		}
	}

	rdr := NewMultiReadSeek0(fnames, true)
	ds := FromCSV(rdr).HasHeader()
	ds.SetFloatVars([]string{"x1", "x2", "prob", "y", "id"})
	ds.SetChunkSize(csize)
	fmt.Printf("Made Dstream with names: %v\n", ds.Names())

	i := 0
	for ds.Next() {
		fmt.Printf("Chunk number %d id variable: %v\n", i, ds.Get("id"))
		i++
	}
	ds.Reset()
	fmt.Println("Reset")
	// A mutating function, scales all values by 2.
	timesTwo := func(x interface{}) {
		v := x.([]float64)
		for i := range v {
			v[i] *= 2
		}
	}
	i = 0
	for ds.Next() {
		fmt.Printf("Chunk number %d x1: %v\n", i, ds.Get("x1"))
		i++
	}
	fmt.Printf("Number of observations: %d\n", ds.NumObs())
	ds.Reset()
	dp := Segment(ds, []string{"id"})
	fmt.Println("Segmented on id")
	dp = Mutate(dp, "x1", timesTwo)
	fmt.Println("Multiplied x1 by 2 using Mutate")
	i = 0
	for dp.Next() {
		fmt.Printf("Chunk number %d x1: %v\n", i, dp.Get("x1"))
		i++
	}
	dp = Drop(dp, []string{"id"})
	fmt.Printf("Dropped id column. Names: %v\n", dp.Names())
	coefs := [][]float64{[]float64{1, 1, 0, 0}}
	dp = Linapply(dp, coefs, "add")
	fmt.Println("Added x1 and x2 with Linapply")
	fmt.Println("Writing entire Dstream to Stdout")
	wr := bufio.NewWriter(os.Stdout)
	err := ToCSV(dp, wr)
	if err != nil {
		fmt.Printf("Failed to write Dstream to Stdout")
	}
	wr.Flush()

	if rmFiles(fnames) != nil {
		fmt.Printf("Error removing temporary data files: %v\n", err)
	}
}
