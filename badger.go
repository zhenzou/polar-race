package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

var (
	dir    string
	thread int
	total  int
	num    int
	size   int

	db *badger.DB
)

func init() {
	flag.StringVar(&dir, "dir", "/data", "dir of badger")
	flag.IntVar(&thread, "thread", 32, "thread num to write")
	flag.IntVar(&num, "num", 10000, "num of kv of each thread")
	flag.IntVar(&size, "size", 4*1024, "value size")

	flag.Parse()

	total = thread * num

	println("dir:", dir)
	println("thread:", thread)
	println("num:", num)
	println("size:", size)
	println("total:", total)
}

func Int64ToBytes(i int64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(i))
	return data
}

func Bytes2Int64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func initBadger() {
	opt := badger.DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir + "/value"
	var err error
	db, err = badger.Open(opt)
	if err != nil {
		panic(err)
	}
}

func newValue(key int64) []byte {
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data, uint64(key))
	return data
}

func runWrite() {
	wg := sync.WaitGroup{}

	for i := 0; i < thread; i++ {
		wg.Add(1)
		go func(j int) {
			for k := j * num; k < j*num+num; k++ {
				txn := db.NewTransaction(true)
				txn.Set(Int64ToBytes(int64(k)), newValue(int64(k)))
				txn.Commit(nil)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func runRead() {
	wg := sync.WaitGroup{}
	for i := 0; i < thread; i++ {
		wg.Add(1)
		go func(j int) {
			for k := j * num; k < j*num+num; k++ {
				key := int64(k)
				txn := db.NewTransaction(false)
				item, err := txn.Get(Int64ToBytes(key))
				if err != nil {
					println(err.Error())
					os.Exit(-1)
				}
				var bs []byte
				item.Value(func(val []byte) {
					bs = val
				})
				value := Bytes2Int64(bs[:8])
				if key != value {
					println(fmt.Sprintf("want %d get %d", key, value))
					os.Exit(-1)
				}
				txn.Commit(nil)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func main() {

	initBadger()
	start := time.Now()

	runWrite()

	writeEnd := time.Now()

	t1 := int(writeEnd.Sub(start).Seconds())

	println(fmt.Sprintf("write:%d qps:%d", t1, total/t1))

	runRead()

	t2 := int(time.Now().Sub(start).Seconds())

	println(fmt.Sprintf("read:%d qps:%d", t2, total/t2))
}
