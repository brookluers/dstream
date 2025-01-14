package dstream

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type MultiReadSeek0 struct {
	curfile *os.File
	ix      int // index of current file
	fnames  []string

	//whether to skip the first record in each file after the first
	sfirst bool
}

//Read reads up to len(p) bytes
//from the current file, advancing to the next
//file if EOF is reached
func (m *MultiReadSeek0) Read(p []byte) (n int, err error) {
	n, err = m.curfile.Read(p)
	if err == io.EOF { // end of current file
		err = m.nextFile()
	}
	return n, err
}

//nextFile moves to the next file in the list of files to read
// skipping the first line if sfirst is true
// returns any error returned by os.Open
// or returns io.EOF if there are no more files to be read
func (m *MultiReadSeek0) nextFile() error {
	var err error
	m.ix++
	fcerr := m.curfile.Close() // close the current file
	if fcerr != nil {
		panic(fcerr)
	}
	if m.ix >= len(m.fnames) { // all files have been read
		return io.EOF
	}
	m.curfile, err = os.Open(m.fnames[m.ix])
	if m.sfirst && err == nil { // discard the first line of this file
		br := bufio.NewReader(m.curfile)
		fline, tmerr := br.ReadBytes('\n')
		if tmerr != nil {
			panic(tmerr)
		}
		m.curfile.Seek(int64(len(fline)), 0) // set the offset to the end of the first line
	}
	return err
}

//Seek can seek back to the start of the first file
//returns error if the offset is not equal to 0
func (m *MultiReadSeek0) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 {
		panic(fmt.Errorf("MultiReadSeek0 can only seek to beginning of first file"))
	}
	var err error
	m.ix = 0
	m.Close()
	m.curfile, err = os.Open(m.fnames[0])
	if err != nil {
		panic(err)
	}
	return 0, nil
}

//Close closes the current file being read
func (m *MultiReadSeek0) Close() error {
	return m.curfile.Close()
}

// NewMultiReadSeek0 returns a new MultiReadSeek0
// that reads from the listed files sequentially and can seek to
// the beginning of the first file
func NewMultiReadSeek0(fnames []string, skipfirst bool) *MultiReadSeek0 {
	cf, err := os.Open(fnames[0])
	if err != nil {
		panic(err)
	}
	return &MultiReadSeek0{
		curfile: cf,
		ix:      0,
		sfirst:  skipfirst,
		fnames:  fnames,
	}
}
