// orderfile32 project main.go
package orderfile32

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/epowsal/orderfile32/orderfiletool"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

func SDBMHash(str []byte) uint64 {
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = uint64(str[i]) + (hash << 6) + (hash << 16) - hash
	}
	return hash
}

func Benchmark1MillionWrite(t *testing.B) {
	orderpath := "testbenchdb/1milliondb"
	OrderFileClear(orderpath)
	db, _ := OpenOrderFile(orderpath, 0, 8, nil)
	db.EnableRmpushLog(false)
	db.SetFlushInterval(9999999)
	ts := time.Now().UnixNano()
	st := ts
	for i := 0; i < 1000000; i++ {
		keybt := orderfiletool.RandPrintChar(13, 13)
		db.RealPush(keybt)
		if i%100000 == 0 {
			ts2 := time.Now().UnixNano()
			fmt.Println(i, (ts2-ts)/int64(time.Millisecond), "Millisecond")
			ts = ts2
		}
	}
	fmt.Println("Push finish time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
}

func Benchmark1MillionRead(t *testing.B) {
	orderpath := "testbenchdb/1milliondb"
	ts := time.Now().UnixNano()
	db, _ := OpenOrderFile(orderpath, 0, 8, nil)
	db.EnableRmpushLog(false)
	db.SetFlushInterval(9999999)
	fmt.Println("Open time:", (time.Now().UnixNano()-ts)/int64(time.Millisecond), "Millisecond")
	ts2 := time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		rdbt := db.RandGet()
		if rdbt == nil {
			panic("error")
		}
		if i%100000 == 0 {
			ts3 := time.Now().UnixNano()
			fmt.Println(i, (ts3-ts2)/int64(time.Millisecond), "Millisecond")
			ts2 = ts2
		}
	}
	fmt.Println("Rand read 1 millin time:", (time.Now().UnixNano()-ts2)/int64(time.Millisecond), "Millisecond")
	fmt.Println("second read")
	ts2 = time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		rdbt := db.RandGet()
		if rdbt == nil {
			panic("error")
		}
		if i%100000 == 0 {
			ts3 := time.Now().UnixNano()
			fmt.Println(i, (ts3-ts2)/int64(time.Millisecond), "Millisecond")
			ts2 = ts2
		}
	}
	fmt.Println("second rand read 1 millin time:", (time.Now().UnixNano()-ts2)/int64(time.Millisecond), "Millisecond")
	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-ts2)/int64(time.Millisecond), "Millisecond")
}

func Benchmark200ThousandWriteBolt(t *testing.B) {
	path := "testbenchdb/blot1milliondb"
	os.Remove(path)
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		panic("error")
	}
	ts := time.Now().UnixNano()
	st := ts

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("testseek")); err != nil {
			panic("error")
			return err
		}

		b := tx.Bucket([]byte("testseek"))

		ff, _ := os.OpenFile("testbenchdb/boltkey", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		for i := 0; i < 200000; i++ {
			keybt := orderfiletool.RandPrintChar(13, 13)
			ff.Write(keybt)
			ff.Write([]byte{'\n'})
			if err := b.Put(keybt[0:8], keybt[8:]); err != nil {
				return err
			}
			if i%100000 == 0 {
				ts2 := time.Now().UnixNano()
				fmt.Println(i, (ts2-ts)/int64(time.Millisecond), "Millisecond")
				ts = ts2
			}
		}
		ff.Close()

		return nil
	}); err != nil {
		panic("error")
	}
	fmt.Println("write finish time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
}

func Benchmark200ThousandReadBolt(t *testing.B) {
	path := "testbenchdb/blot1milliondb"
	ts := time.Now().UnixNano()
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		panic("error")
	}
	fmt.Println("Open time:", (time.Now().UnixNano()-ts)/int64(time.Millisecond), "Millisecond")
	ts2 := time.Now().UnixNano()
	ts4 := ts2
	i := 0
	rdm := make(map[string][]byte, 0)
	ctt, ctte := ioutil.ReadFile("testbenchdb/boltkey")
	if ctte == nil {
		keyls := bytes.Split(ctt, []byte{'\n'})
		for i := 0; i < len(keyls); i++ {
			if len(keyls[i]) != 13 {
				continue
			}
			rdm[string(keyls[i][:8])] = keyls[i][8:]
		}
		fmt.Println("rdm length:", len(rdm))
		for k, _ := range rdm {
			if err := db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("testseek"))
				v := b.Get([]byte(k))
				if v == nil {
					panic("error")
				}
				return nil
			}); err != nil {
				panic("error")
			}
			if i%100000 == 0 {
				fmt.Println("key:", string(k))
				ts3 := time.Now().UnixNano()
				fmt.Println(i, (ts3-ts2)/int64(time.Millisecond), "Millisecond")
				ts2 = ts2
			}
			i += 1

		}
	}

	fmt.Println("read finish time:", (time.Now().UnixNano()-ts4)/int64(time.Millisecond), "Millisecond")
	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-ts4)/int64(time.Millisecond), "Millisecond")
}

func Benchmark1MillionBadgerWrite(t *testing.B) {
	opts := badger.DefaultOptions("testbenchdb/badgerdb")
	db, err := badger.Open(opts)
	if err != nil {
		panic("error")
	}
	ts := time.Now().UnixNano()
	st := ts
	ff, _ := os.OpenFile("testbenchdb/badgerkey", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	for i := 0; i < 1000000; i++ {
		keybt := orderfiletool.RandPrintChar(13, 13)

		err := db.Update(func(txn *badger.Txn) error {
			err := txn.Set(keybt[:8], keybt[8:])
			return err
		})
		if err != nil {
			panic(err)
		}

		ff.Write(keybt)
		ff.Write([]byte{'\n'})
		if i%100000 == 0 {
			ts2 := time.Now().UnixNano()
			fmt.Println(i, (ts2-ts)/int64(time.Millisecond), "Millisecond")
			ts = ts2
		}
	}
	ff.Close()

	fmt.Println("Push finish time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-st)/int64(time.Millisecond), "Millisecond")
}

func Benchmark1MillionBadgerRead(t *testing.B) {
	ts := time.Now().UnixNano()
	opts := badger.DefaultOptions("testbenchdb/badgerdb")
	db, err := badger.Open(opts)
	if err != nil {
		panic("error")
	}
	fmt.Println("Open time:", (time.Now().UnixNano()-ts)/int64(time.Millisecond), "Millisecond")
	ts2 := time.Now().UnixNano()
	ts4 := ts2
	i := 0
	rdm := make(map[string][]byte, 0)
	ctt, ctte := ioutil.ReadFile("testbenchdb/badgerkey")
	if ctte == nil {
		keyls := bytes.Split(ctt, []byte{'\n'})
		for i := 0; i < len(keyls); i++ {
			if len(keyls[i]) != 13 {
				continue
			}
			rdm[string(keyls[i][:8])] = keyls[i][8:]
		}
		fmt.Println("rdm length:", len(rdm))
		ts2 = time.Now().UnixNano()
		ts4 = ts2
		for k, _ := range rdm {
			err := db.View(func(txn *badger.Txn) error {
				_, err := txn.Get([]byte(k))
				if err != nil {
					fmt.Println(err)
					panic("error")
				}
				return nil
			})
			if err != nil {
				panic("error")
			}
			if i%100000 == 0 {
				fmt.Println("key:", string(k))
				ts3 := time.Now().UnixNano()
				fmt.Println(i, (ts3-ts2)/int64(time.Millisecond), "Millisecond")
				ts2 = ts2
			}
			i += 1

		}
	}

	fmt.Println("read finish time:", (time.Now().UnixNano()-ts4)/int64(time.Millisecond), "Millisecond")

	fmt.Println("second read")
	ts2 = time.Now().UnixNano()
	ts4 = ts2
	for k, _ := range rdm {
		err := db.View(func(txn *badger.Txn) error {
			_, err := txn.Get([]byte(k))
			if err != nil {
				fmt.Println(err)
				panic("error")
			}
			return nil
		})
		if err != nil {
			panic("error")
		}
		if i%100000 == 0 {
			fmt.Println("key:", string(k))
			ts3 := time.Now().UnixNano()
			fmt.Println(i, (ts3-ts2)/int64(time.Millisecond), "Millisecond")
			ts2 = ts2
		}
		i += 1

	}
	fmt.Println("second read finish time:", (time.Now().UnixNano()-ts4)/int64(time.Millisecond), "Millisecond")

	db.Close()
	fmt.Println("Close time:", (time.Now().UnixNano()-ts4)/int64(time.Millisecond), "Millisecond")
}
