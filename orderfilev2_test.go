package orderfilemem

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/epowsal/orderfile32/orderfiletool"
)

func TestFillKeyEndByte(t *testing.T) {
	orderpath := "testdb/TestFillKeyEndByte"
	OrderFileClear(orderpath)
	db, _ := OpenOrderFile(orderpath, 0, 0, []byte{0})
	db.PushKey([]byte("/a/b/c"), []byte{})
	_, bfill := db.FillKey([]byte("/a"))
	if bfill {
		panic("fill key error.")
	}
	_, bfill = db.FillKey([]byte("/a/b/c"))
	if !bfill {
		panic("fill key error.")
	}
	db.Flush()
	_, bfill = db.FillKey([]byte("/a"))
	if bfill {
		panic("fill key error.")
	}
	_, bfill = db.RealFill([]byte("/a/b/c"))
	if !bfill {
		panic("fill key error.")
	}
	_, bfill = db.RealFill([]byte("/a/b/c\x00"))
	if !bfill {
		panic("fill key error.")
	}
	_, bfill = db.FillKey([]byte("/a/b/c"))
	if !bfill {
		panic("fill key error.")
	}

	_, bfill = db.FillKey([]byte("/a/b/c\x00"))
	if bfill {
		panic("fill key error.")
	}

	db.Close()
	OrderFileClear(orderpath)
}

func genlongkey() []byte {
	wordbt := make([]byte, 0)
	genlen := 1<<17 + rand.Intn(512)
	for i := 0; i < genlen; i++ {
		wordbt = append(wordbt, byte(0x41+rand.Intn(26)))
	}
	return wordbt
}

func TestLongKey(t *testing.T) {
	orderpath := "testdb/testlongkey"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof, _ := OpenOrderFile(orderpath, 0, 0, []byte{0})

	dd := make(map[string]int, 0)
	for i := 0; i < 100; i++ {
		lkey := genlongkey()
		testof.RealPush(lkey)
		dd[string(lkey)] = 0
	}

	for key, _ := range dd {
		_, bgetlkey := testof.RealFill([]byte(key))
		if bgetlkey == false {
			panic("error")
		}
	}

	for key, _ := range dd {
		brm := testof.RealRm([]byte(key))
		if brm == false {
			panic("error")
		}
	}

	for key, _ := range dd {
		_, bgetlkey := testof.RealFill([]byte(key))
		if bgetlkey == true {
			panic("error")
		}
	}

	testof.Close()
}

func TestRmKeyFillKey(t *testing.T) {
	orderpath := "testdb/TestRmKeyFillKey"
	OrderFileClear(orderpath)
	db, _ := OpenOrderFile(orderpath, 0, 0, []byte{0})
	db.PushKey([]byte("/a/b/c"), []byte{})
	brm := db.RmKey([]byte("/a/b/c"))
	if !brm {
		panic("RmKey error.")
	}
	_, bfill := db.FillKey([]byte("/a/b/c"))
	if bfill {
		panic("fill key error.")
	}
	db.PushKey([]byte("/a/b/c"), []byte{})
	db.Flush()
	brm = db.RmKey([]byte("/a/b/c"))
	if !brm {
		panic("RmKey error.")
	}

	_, bfill = db.FillKey([]byte("/a/b/c"))
	if bfill {
		panic("fill key error.")
	}
	db.Close()
	OrderFileClear(orderpath)
}

func TestBeforeErrorDb(t *testing.T) {
	orderpath := "testdb/TestBeforeErrorDb"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	testof, _ := OpenOrderFile(orderpath, 0, 0, nil)
	curkey := []byte{}
	for true {
		nextkey2, bnextkey := testof.NextKey(curkey)
		if !bnextkey {
			break
		}
		fmt.Println("nextkey2:", string(nextkey2))
		curkey = nextkey2
	}
	testof.PushKey([]byte("http://sc.cri.cn/20180821/5214ce44-1b57-c896-f787-b11c24a5a790.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20180831/e452aacc-861f-24e0-9ea2-b8171463b890.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20180831/e4134430-fd3d-fa5c-ba3d-8ed2165ff671.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20180927/926bda3b-7652-9bc3-b935-a9dd965cfb8e.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20180815/45a95277-367c-3bb4-e6df-67433b894caf.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20181015/65b985fe-3c96-acec-3bbc-730ddf2c5ec8.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20181119/ea692cb9-b26b-e502-72e4-e9d47c2613f1.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190107/fb4abf5d-8129-6be5-44f6-195d66929e8c.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190118/44e92df2-5b1c-aa96-d19c-234ab01f252a.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190123/eba6b60a-ecf2-57ac-c4b9-b1e66923bb72.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190125/a0bcf08d-3b82-3bb5-c1dd-f585aea5afc6.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190201/4e0910bd-26d1-a319-c377-3d5ab327b944.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190109/f8423905-9d2c-6dd0-ed1e-f2d652272017.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190118/324339c3-bf82-45ba-b937-c6caa7267a6b.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190131/b90c6090-ebf9-985a-5dee-3f22c61af96a.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/8065cbb2-a204-3f88-3690-f4b48460ca7e.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190131/8fd0ad95-f7b1-366a-b153-56e85f0d2293.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190201/8160c5bf-e5c7-9f8e-716b-6b5f3378bc06.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190201/352c3ac1-28e1-5a03-aa07-bab22560b182.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/9abceede-a248-a083-2017-8b655b9ec969.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testof.PushKey([]byte("http://sc.cri.cn/20190202/6a6a7224-357d-93b5-c9cc-c71d12f567e5.html"), []byte{})
	testof.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeyUseLoad(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKeyUseLoad"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	//seed = 1630847798
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	endbt := []byte{0}
	testof, testofer := OpenOrderFile(orderpath, 0, 0, endbt)
	allword2 := []string{}
	allword2map := make(map[string]string, 0)
	for i := 0; i < 10000; i++ {
		word2 := genword()
		word2 = append(word2, byte(0))
		word3 := genword()
		_, be := allword2map[string(word2)]
		if !be {
			testof.PushKey(word2, word3)
			allword2 = append(allword2, string(word2))
			allword2map[string(word2)] = string(word3)
			time.Sleep(15 * time.Microsecond)
		}
	}
	testof.Flush()
	testof.Close()

	testof, testofer = OpenOrderFile(orderpath, 0, 0, nil)
	if testofer != nil {
		fmt.Println(testofer)
		panic("error")
	}
	sort.Strings(allword2)
	for key, val := range allword2map {
		nextkey, bfound := testof.PreviousKey([]byte(key))
		bfnd := false
		for ind, _ := range allword2 {
			if allword2[ind] == key {
				if ind-1 >= 0 && allword2[ind-1] != string([]byte(nextkey)[:len([]byte(allword2[ind-1]))]) {
					fmt.Println("key val", key, "val:", val, "found:", string(nextkey), bfound, " should:", allword2[ind-1], " getpart:", string([]byte(nextkey)[:len([]byte(key))]))
					panic("previous key error!")
				}
				bfnd = true
				break
			}
		}
		if bfnd == false && key != allword2[0] {
			_, bfill := testof.FillKey([]byte(key))
			if bfill == false {
				fmt.Println("key val", key, "val:", val, "found:", string(nextkey), bfound)
				panic("error")
			}
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
	return
}

func TestCount1(t *testing.T) {
	orderpath := "testdb/testcount1"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	odb, _ := OpenOrderFile(orderpath, 0, 0, nil)
	ff, _ := os.OpenFile("sitelist.txt", os.O_RDONLY, 0666)
	nrd := bufio.NewReader(ff)
	totaladd := int64(0)
	urlist := []string{}
	for true {
		line, _, le := nrd.ReadLine()
		if le != nil {
			break
		}
		line = bytes.Trim(line, "\r\n\t ")
		if totaladd == 38 {
			fmt.Println("ksdjfd")
		}
		if orderfiletool.SliceSearch(urlist, string(line), 0) != -1 {
			continue
		}
		odb.RealPush(line)
		totaladd += 1
		if odb.Count() < totaladd {
			fmt.Println(totaladd, string(line))
			panic("error")
		}
		fmt.Println(totaladd)
		urlist = append(urlist, string(line))
	}
	fmt.Println(odb.Count(), totaladd)
	if odb.Count() != totaladd {
		panic("error")
	}
	ff.Close()
	odb.Close()
	OrderFileClear(orderpath)
}

func TestCrash2(t *testing.T) {
	orderpath := "testdb/testcrash2"
	OrderFileClear(orderpath)
	var testdb *OrderFile
	testdb, _ = OpenOrderFile(orderpath, 0, 8, nil)
	testdb.RealPush([]byte("84757264test text"))
	key, bok := testdb.FillKey([]byte("84757264test text"))
	fmt.Println("FillKey", string(key), bok)
	testdb.Close()

	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	key, bok = testdb.FillKey([]byte("84757264test text"))
	fmt.Println("FillKey", string(key), bok)
	if string(key) != "84757264test text" {
		panic("error")
	}
	brm := testdb.RealRm([]byte("84757264"))
	if brm == false {
		panic("error")
	}
	brm = testdb.RealRm([]byte("sdjkfsdhfjk"))
	if brm == true {
		panic("error")
	}
	_, bok = testdb.FillKey([]byte("84757264"))
	if bok == true {
		panic("error")
	}

	fmt.Println("randget:")
	fmt.Println(string(testdb.RandGet()))

	testdb.Close()
	OrderFileClear(orderpath)
}

func TestBinError5(t *testing.T) {
	//allowmultiopenfortest = true
	go func() {
		//http.ListenAndServe("0.0.0.0:80", nil)
	}()

	orderpath := "testdb/binerror5"
	OrderFileClear(orderpath)
	var testdb *OrderFile
	tempcompref, tempcompreferr := os.OpenFile("_curdir.headdb_err5.log", os.O_RDONLY, 0666)
	if tempcompreferr == nil {
		endpos, _ := tempcompref.Seek(0, os.SEEK_END)
		if endpos > 0 {
			// bp := false
			// ee := []byte{}
			// bnn := false
			tempcompref.Seek(0, os.SEEK_SET)
			readfrom := int64(0)
			curi := int64(0)
			readlen := int64(0)
			databuf := make([]byte, 1<<25)
			totaodocnt := 0
			for i := int64(0); i < endpos; {
				readlen = int64(len(databuf)) - readfrom
				if i+readlen > endpos {
					readlen = endpos - i
				}
				_, readerr := tempcompref.Read(databuf[readfrom : readfrom+readlen])
				if readerr != nil {
					panic(" read newsavedata have error.")
					break
				}
				curi = int64(0)
				for true {
					totaodocnt += 1
					if totaodocnt%3000 == 0 {
						fmt.Println("op count:", totaodocnt, curi, readfrom+readlen, i)
					}
					if curi+int64(len("O"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("O")) {
						oplen := int64(len("O"))
						rmkeylen := binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4])
						if curi+oplen+4+int64(rmkeylen) < readfrom+readlen {
							curi += oplen + 4 + int64(rmkeylen)
							testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
							testdb.SetFlushInterval(9999999999999)
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else if curi+int64(len("F"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("F")) {
						curi += int64(len("F"))
						curi += 4
						testdb.Flush()
						//time.Sleep(30 * time.Second)
						fmt.Println("real flush positin end")
					} else if curi+int64(len("C"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("C")) {
						oplen := int64(len("C"))
						pathlen := int64(binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4]))
						if curi+oplen+4+pathlen < readfrom+readlen {
							curi += oplen + 4 + pathlen
							testdb.Close()
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else if curi+int64(len("P"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("P")) {
						oplen := int64(len("P"))
						pushkeylen := binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4])
						if curi+oplen+4+int64(pushkeylen) < readfrom+readlen {
							pushkey := databuf[curi+oplen+4 : curi+oplen+4+int64(pushkeylen)]
							testdb.RealPush(pushkey)
							curi += oplen + 4 + int64(pushkeylen)
							//time.Sleep(time.Second)
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else if curi+int64(len("RP"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("RP")) {
						oplen := int64(len("RP"))
						keylen := binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4])
						if curi+oplen+4+int64(keylen)+4 < readfrom+readlen {
							key := databuf[curi+oplen+4 : curi+oplen+4+int64(keylen)]
							vallen := binary.BigEndian.Uint32(databuf[curi+oplen+4+int64(keylen) : curi+oplen+4+int64(keylen)+4])
							if curi+oplen+4+int64(keylen)+4+int64(vallen) < readfrom+readlen {
								val := databuf[curi+oplen+4+int64(keylen)+4 : curi+oplen+4+int64(keylen)+4+int64(vallen)]

								// if bytes.Index([]byte{0, 0, 0, 0, 0, 159, 217, 110, 0, 0, 0, 0, 217, 248, 8, 38, 0, 23, 132, 164, 0}, append(key, val...)) != -1 {
								// 	bp = true
								// 	ee = BytesCombine(key, val)
								// }
								testdb.RealRmPush(key, val)
								// if bp && bnn == false {
								// 	_, bn := testdb.NextKey(ee)
								// 	if bn {
								// 		bnn = true
								// 	}
								// }
								// if bnn {
								// 	testdb.NextKey(ee)
								// }
								curi += oplen + 4 + int64(keylen) + 4 + int64(vallen)
							} else {
								copy(databuf, databuf[curi:])
								readfrom = readfrom + readlen - curi
								break
							}
							//time.Sleep(time.Second)
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else if curi+int64(len("R"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("R")) {
						oplen := int64(len("R"))
						rmkeylen := binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4])
						if curi+oplen+4+int64(rmkeylen) < readfrom+readlen {
							rmkey := databuf[curi+oplen+4 : curi+oplen+4+int64(rmkeylen)]
							testdb.RealRm(rmkey)
							curi += oplen + 4 + int64(rmkeylen)
							//time.Sleep(time.Second)
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else if curi+int64(len("P"))+4 < readfrom+readlen && bytes.HasPrefix(databuf[curi:], []byte("P")) {
						oplen := int64(len("P"))
						rmkeylen := binary.BigEndian.Uint32(databuf[curi+oplen : curi+oplen+4])
						if curi+oplen+4+int64(rmkeylen) < readfrom+readlen {
							rmkey := databuf[curi+oplen+4 : curi+oplen+4+int64(rmkeylen)]
							testdb.PreviousKey(rmkey)
							curi += oplen + 4 + int64(rmkeylen)
						} else {
							copy(databuf, databuf[curi:])
							readfrom = readfrom + readlen - curi
							break
						}
					} else {
						copy(databuf, databuf[curi:])
						readfrom = readfrom + readlen - curi
						break
					}
				}
				i += readlen
			}
		}
		tempcompref.Close()
	}

	if testdb != nil {
		fmt.Println("count:", testdb.Count())

		ncurkey := []byte{}
		nextcnt := 0
		for true {
			nextkey, bnext := testdb.NextKey(ncurkey)
			if !bnext {
				break
			}
			ncurkey = nextkey
			nextcnt += 1
		}
		fmt.Println("nextcnt:", nextcnt)

		previouscnt := 0
		pcurkey := []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")
		for true {
			prekey, bnext := testdb.PreviousKey(pcurkey)
			if !bnext {
				break
			}
			pcurkey = prekey
			previouscnt += 1
		}
		fmt.Println("previouscnt:", previouscnt)
		if nextcnt != previouscnt {
			panic("error")
		}

		testval := uint64(1407435246)
		testvalbt := make([]byte, 8)
		binary.BigEndian.PutUint64(testvalbt, testval)
		fmt.Println("fillval", testvalbt)
		fillval, bfill := testdb.RealFill(testvalbt)
		if bfill == false {
			fillval, bfill = testdb.PreviousKey(testvalbt)
			if bfill == false {
				fmt.Println("fillval, bfill", fillval, bfill, testdb.Count())
				panic("error")
			} else {
				fmt.Println("previous value", fillval, bfill)
			}
		}

		orderstart := binary.BigEndian.Uint64(fillval[:8])
		part2 := binary.BigEndian.Uint32(fillval[16 : 16+4])
		orderlen := uint64(part2 >> 7)
		if !(1407435246 >= orderstart && 1407435246 < orderstart+orderlen) {
			fmt.Println("fillval", fillval, binary.BigEndian.Uint64(fillval[:8]))
			panic("error")
		}

		testdb.Close()
	}
}

func TestCHildChainFill(t *testing.T) {
	odb2path := "testdb/CHildChainFill"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)
	a := "aaaabbbb"
	b := "aaaabbbbccccdddd"
	c := "aaaabbbbccccddddeeeeffff"
	d := "aaaabbbbccccddddeeeeffffgggghhhh"
	e := "aaaabbbbccccddddeeeeffffgggghhhhiiiiiiii"
	f := "aaaabbbbccccddddeeeeffffgggghhhhiiiiiiiijjjjjjj"
	g := "aaaabbbbccccddddeeeeffffgggghhhhiiiiiiiijjjjjjjmmmmmmmm"
	odb2.RealPush([]byte(a))
	odb2.RealPush([]byte(b))
	odb2.RealPush([]byte(c))
	odb2.RealPush([]byte(d))
	odb2.RealPush([]byte(e))
	odb2.RealPush([]byte(f))
	odb2.RealPush([]byte(g))
	odb2.RealRm([]byte(g))
	_, bfillg := odb2.RealFill([]byte(g))
	if bfillg == true || odb2.Count() != 6 {
		fmt.Println("count:", odb2.Count(), "bfillg", bfillg)
		panic("error")
	}
	odb2.RealRm([]byte(a))
	odb2.RealRm([]byte(b))
	odb2.RealRm([]byte(c))
	odb2.RealRm([]byte(d))
	odb2.RealRm([]byte(e))
	_, bfa := odb2.RealFill([]byte(a))
	if bfa == false {
		panic("error")
	}
	_, bfb := odb2.RealFill([]byte(b))
	if bfb == false {
		panic("error")
	}
	_, bfc := odb2.RealFill([]byte(c))
	if bfc == false {
		panic("error")
	}
	_, bfd := odb2.RealFill([]byte(d))
	if bfd == false {
		panic("error")
	}
	ef, bfe := odb2.RealFill([]byte(e))
	if bfe == false {
		fmt.Println("ef", string(ef), bfe)
		panic("error")
	}
	_, bff := odb2.RealFill([]byte(f))
	if bff == false {
		panic("error")
	}
	fill, bfill := odb2.RealFill([]byte(b))
	fmt.Println(string(fill), bfill)
	if string(fill) != f {
		panic("error")
	}
	prekey, bprekey := odb2.PreviousKey([]byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"))
	fmt.Println(string(prekey), bprekey)
	if string(prekey) != f {
		panic("error")
	}
	nextkey, bnext := odb2.NextKey([]byte{})
	fmt.Println(string(nextkey), bnext)
	if string(nextkey) != f {
		panic("error")
	}
	if odb2.Count() != 1 {
		fmt.Println("odb2.Count():", odb2.Count())
		panic("error")
	}
	odb2.RealRm([]byte(f))
	if odb2.Count() != 0 {
		fmt.Println("odb2.Count():", odb2.Count())
		panic("error")
	}
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestFillPreError3(t *testing.T) {
	odb2path := "testdb/FillPreError1"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)

	ff, _ := os.OpenFile("testfilpreerror3.log", os.O_RDONLY, 0666)
	ffbo := bufio.NewReader(ff)
	prestart := int64(0)
	linenum := 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("orderseginfo", linenum)
			panic(r)
		}
	}()
	totalcnt := 0
	for true {
		line, _, linee := ffbo.ReadLine()
		if linee != nil {
			break
		}
		line = bytes.Trim(line, "\r\n\t ")
		linels := bytes.Split(line, []byte(" "))
		if len(linels) != 4 {
			fmt.Println("line ", string(line))
			panic("error")
			continue
		}
		var orderstart, orderlen, datapos, dataposlen uint64
		orderstart, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		datapos, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		dataposlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderseginfo := make([]byte, 20)
		binary.BigEndian.PutUint64(orderseginfo, orderstart)
		binary.BigEndian.PutUint64(orderseginfo[8:8+8], (datapos<<11)|(orderlen>>11))
		binary.BigEndian.PutUint32(orderseginfo[16:16+4], uint32((dataposlen<<11)|(orderlen&0x7FF)))
		linenum += 1
		oldcnt := odb2.Count()
		odb2.RealPush(orderseginfo)
		if odb2.Count() == oldcnt {
			fmt.Println("line ", string(line))
			panic("error")
		}
		if int64(orderstart) < prestart {
			panic("error")
		}
		prestart = int64(orderstart)
		totalcnt += 1
	}
	if odb2.Count() != 13963 {
		fmt.Println("odb2.Count():", odb2.Count(), totalcnt)
		panic("error")
	}
	orderstart2 := uint64(167833135)
	orderstart2bt := make([]byte, 8)
	binary.BigEndian.PutUint64(orderstart2bt, orderstart2)
	fmt.Println("should", orderstart2bt)

	fmt.Println("fill should")
	fmt.Println(odb2.RealFill(orderstart2bt))

	testval := uint64(167839321)
	testvalbt := make([]byte, 8)
	binary.BigEndian.PutUint64(testvalbt, testval)
	fmt.Println("fillval", testvalbt)
	fillval, bfill := odb2.RealFill(testvalbt)
	if bfill == false {
		fillval, bfill = odb2.PreviousKey(testvalbt)
		if bfill == false {
			fmt.Println("fillval, bfill", fillval, bfill, odb2.Count())
			panic("error")
		}
	}
	if bytes.Compare(fillval[:len(orderstart2bt)], orderstart2bt) != 0 {
		fmt.Println("fillval", fillval, binary.BigEndian.Uint64(fillval[:8]))
		panic("error")
	}

	odb2.Close()
	OrderFileClear(odb2path)
}

func TestFillPreError2(t *testing.T) {
	odb2path := "testdb/FillPreError1"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)

	ff, _ := os.OpenFile("testfilpreerror2.log", os.O_RDONLY, 0666)
	ffbo := bufio.NewReader(ff)
	prestart := int64(0)
	linenum := 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("orderseginfo", linenum)
			panic(r)
		}
	}()
	for true {
		line, _, linee := ffbo.ReadLine()
		if linee != nil {
			break
		}
		line = bytes.Trim(line, "\r\n\t ")
		linels := bytes.Split(line, []byte(" "))
		if len(linels) != 4 {
			continue
		}
		var orderstart, orderlen, datapos, dataposlen uint64
		orderstart, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		datapos, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		dataposlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderseginfo := make([]byte, 20)
		binary.BigEndian.PutUint64(orderseginfo, orderstart)
		binary.BigEndian.PutUint64(orderseginfo[8:8+8], (datapos<<11)|(orderlen>>11))
		binary.BigEndian.PutUint32(orderseginfo[16:16+4], uint32((dataposlen<<11)|(orderlen&0x7FF)))
		linenum += 1
		odb2.RealPush(orderseginfo)
		if int64(orderstart) < prestart {
			panic("error")
		}
		prestart = int64(orderstart)
	}
	if odb2.Count() != 6057 {
		fmt.Println("odb2.Count():", odb2.Count())
		panic("error")
	}
	orderstart2 := uint64(67170978)
	orderstart2bt := make([]byte, 8)
	binary.BigEndian.PutUint64(orderstart2bt, orderstart2)
	fmt.Println("should", orderstart2bt)

	testval := uint64(67180859)
	testvalbt := make([]byte, 8)
	binary.BigEndian.PutUint64(testvalbt, testval)
	fmt.Println("fillval", testvalbt)
	fillval, bfill := odb2.RealFill(testvalbt)
	if bfill == false {
		fillval, bfill = odb2.PreviousKey(testvalbt)
		if bfill == false {
			fmt.Println("fillval, bfill", fillval, bfill, odb2.Count())
			panic("error")
		}
	}
	if bytes.Compare(fillval[:len(orderstart2bt)], orderstart2bt) != 0 {
		fmt.Println("fillval", fillval, binary.BigEndian.Uint64(fillval[:8]))
		panic("error")
	}
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestFillPreError(t *testing.T) {
	odb2path := "testdb/FillPreError1"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)

	ff, _ := os.OpenFile("testfilpreerror1.log", os.O_RDONLY, 0666)
	ffbo := bufio.NewReader(ff)
	prestart := int64(-1)
	for true {
		line, _, linee := ffbo.ReadLine()
		if linee != nil {
			break
		}
		line = bytes.Trim(line, "\r\n\t ")
		linels := bytes.Split(line, []byte(" "))
		if len(linels) != 4 {
			continue
		}
		var orderstart, orderlen, datapos, dataposlen uint64
		orderstart, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		datapos, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		dataposlen, _ = strconv.ParseUint(string(linels[0]), 10, 64)
		orderseginfo := make([]byte, 20)
		binary.BigEndian.PutUint64(orderseginfo, orderstart)
		binary.BigEndian.PutUint64(orderseginfo[8:8+8], (datapos<<11)|(orderlen>>11))
		binary.BigEndian.PutUint32(orderseginfo[16:16+4], uint32((dataposlen<<11)|(orderlen&0x7FF)))
		odb2.RealPush(orderseginfo)
		if int64(orderstart) < prestart {
			panic("error")
		}
		prestart = int64(orderstart)
	}
	orderstart2 := uint64(887869754)
	orderstart2bt := make([]byte, 8)
	binary.BigEndian.PutUint64(orderstart2bt, orderstart2)
	fmt.Println("should", orderstart2bt)

	testval := uint64(887881915)
	testvalbt := make([]byte, 8)
	binary.BigEndian.PutUint64(testvalbt, testval)
	fillval, bfill := odb2.RealFill(testvalbt)
	if bfill == false {
		fillval, bfill = odb2.PreviousKey(testvalbt)
		if bfill == false {
			fmt.Println("fillval, bfill", fillval, bfill, odb2.Count())
			panic("error")
		}
	}
	orderstart := uint64(887882163)
	orderstartbt := make([]byte, 8)
	binary.BigEndian.PutUint64(orderstartbt, orderstart)
	if bytes.Compare(fillval[:len(orderstartbt)], orderstartbt) == 0 {
		panic("error")
	}
	if bytes.Compare(fillval[:len(orderstartbt)], orderstart2bt) != 0 {
		fmt.Println("fillval", fillval)
		panic("error")
	}
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestLkkkkk(t *testing.T) {
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	odb2path := "testdb/testkkkkk"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)
	fmt.Println(odb2.RealPush([]byte{}))
	fmt.Println(odb2.Count())
	if odb2.Count() != 1 {
		panic("error")
	}
	fmt.Println(odb2.RealFill([]byte{}))
	_, bf := odb2.RealFill([]byte{})
	if bf == false {
		panic("error")
	}
	fmt.Println(odb2.RealRm([]byte{}))
	fmt.Println(odb2.Count())
	if odb2.Count() != 0 {
		panic("erro")
	}
	fmt.Println(odb2.RealFill([]byte{}))
	_, bf = odb2.RealFill([]byte{})
	if bf == true {
		panic("error")
	}
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestHaveErrorList(t *testing.T) {
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	totalcnt := int64(0)
	odb2path := "testdb/testcountout2"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 4, 0, nil)
	odb2.SetFlushInterval(99999999999)
	li := []string{}
	li2 := &sync.Map{}
	li3 := &sync.Map{}
	li4 := &sync.Map{}
	//ff, _ := os.OpenFile("fortest_sitelist.txt", os.O_RDONLY, 0666)
	ff, _ := os.OpenFile("dictionary.txt", os.O_RDONLY, 0666)
	//ff, _ := os.OpenFile("dictionary.reverse.txt", os.O_RDONLY, 0666)
	nrd := bufio.NewReader(ff)
	emptycnt := 0
	realcnt := 0
	for true {
		line, le := nrd.ReadBytes(byte('\n'))
		if le != nil {
			break
		}
		line = bytes.Trim(line, "\r\n\t ")
		if len(line) == 0 {
			emptycnt += 1
		}
		totalcnt += 1
		oldline := BytesClone(line)
		_, bold := li2.Load(string(line))
		odb2.RealPush(line)
		filk3, errofill3 := odb2.FillKey([]byte("\"tzc"))
		if errofill3 == true && bytes.Compare(filk3, []byte("\"tzc")) == 0 {
			fmt.Println(totalcnt, "filk:", filk3, line)
			panic("unknow error.")
		}
		filk, errofill := odb2.FillKey([]byte("\""))
		if errofill == true && bytes.Compare(filk, []byte("\"")) == 0 {
			fmt.Println(totalcnt, "filk:", filk, line)
			panic("unknow error.")
		}
		filk2, errofill2 := odb2.FillKey([]byte("\"0"))
		if errofill2 == true && bytes.Compare(filk, []byte("\"0")) == 0 {
			fmt.Println(totalcnt, "filk:", filk2, line)
			panic("unknow error.")
		}

		if !bold {
			realcnt += 1
		}
		if odb2.Count() != int64(realcnt) {
			fmt.Println("totalcnt", totalcnt)
			panic("error")
		}
		if bytes.Compare(oldline, line) != 0 {
			panic("error")
		}
		li = append(li, string(line))
		li2.Store(string(line), 1)
		li3.Store(string(line), 1)
		li4.Store(string(line), 1)
	}
	odb2.Flush()
	cnt := odb2.Count()
	fmt.Println("dbcount", cnt, "realcnt", realcnt)
	if cnt != int64(realcnt) {
		panic("error")
	}

	fmt.Println("randget:")
	fmt.Println(string(odb2.RandGet()))

	morekey1 := []string{}
	var curkey []byte = nil
	cnt2 := 0
	totalcnt2 := 0
	for true {
		nextkey, bnext := odb2.NextKey(curkey)
		if !bnext {
			break
		}
		totalcnt2 += 1
		if string(nextkey) == "\"tzc" {
			fmt.Println(odb2.FillKey([]byte("\"tzc")))
			fmt.Println(totalcnt2)
			panic("error")
		}
		_, bload := li3.Load(string(nextkey))
		if bload {
			li3.Delete(string(nextkey))
		} else {
			morekey1 = append(morekey1, string(nextkey))
		}
		curkey = nextkey
		cnt2 += 1
	}
	li3.Range(func(key, val interface{}) bool {
		fmt.Println("li3 remain key", key.(string))
		if len(key.(string)) != 0 {
			panic("error")
		}
		return true
	})
	fmt.Println("emptycnt", emptycnt)
	if cnt2 != int(realcnt) {
		fmt.Println("morekey1:", len(morekey1), morekey1)
		fmt.Println("cnt error:", cnt2, int(realcnt))
		panic("error")
	}

	morekey2 := []string{}
	curkey2 := []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")
	cnt33 := 0
	totalcnt3 := 0
	for true {
		nextkey, bnext := odb2.PreviousKey(curkey2)
		if !bnext {
			break
		}
		totalcnt3 += 1
		if string(nextkey) == "\"tzc" {
			fmt.Println(odb2.FillKey([]byte("\"tzc")))
			fmt.Println(totalcnt3)
			panic("error")
		}
		_, bload := li2.Load(string(nextkey))
		if bload {
			li2.Delete(string(nextkey))
		} else {
			morekey2 = append(morekey2, string(nextkey))
		}
		curkey2 = nextkey
		cnt33 += 1
	}
	li2.Range(func(key, val interface{}) bool {
		fmt.Println("li2 remain key", key.(string))
		panic("error")
		return true
	})
	fmt.Println("emptycnt", emptycnt)
	if cnt33 != int(realcnt) {
		fmt.Println("morekey2:", len(morekey2), morekey2)
		fmt.Println("cnt error:", cnt33, int(realcnt))
		panic("error")
	}

	for kkind, kk := range li {
		kk2 := kk
		_, bfik := odb2.FillKey([]byte(kk))
		if !bfik {
			panic("error")
		}
		odb2.RealPush([]byte(kk))
		if kk2 != kk {
			fmt.Println("kk2", kk2)
			fmt.Println("kk", kk)
			panic("error")
		}
		if odb2.Count() > int64(realcnt) {
			fmt.Println(kkind, string(kk))
			panic("error")
		}
	}
	if odb2.Count() != int64(realcnt) {
		panic("error")
	}

	li4.Range(func(key, val interface{}) bool {
		odb2.RealRm([]byte(key.(string)))
		realcnt -= 1
		if odb2.Count() != int64(realcnt) {
			panic("error")
		}
		return true
	})
	fmt.Println("4 totalcnt:", totalcnt, "dbcount", odb2.Count())
	ff.Close()
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestMaxMatch(t *testing.T) {
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	odb2path := "testdb/testmaxmatch"
	OrderFileClear(odb2path)
	odb2, _ := OpenOrderFile(odb2path, 0, 0, nil)
	odb2.RealPush([]byte("科技"))
	odb2.RealPush([]byte("科学"))
	odb2.RealPush([]byte("科技发展"))
	odb2.RealPush([]byte("科技发生"))
	odb2.RealPush([]byte("科技发展国家"))
	odb2.RealPush([]byte("科技发展国策略"))
	odb2.RealPush([]byte("科技发展国家事业"))
	fmt.Println("maxmatch wordmavec:")
	artical := []byte("科技发展国家事业回去国")
	mavec := odb2.MaxMatch(artical)
	fmt.Println(mavec)
	for i := 0; i < len(mavec); i++ {
		fmt.Println(string(artical[0 : 0+mavec[i]]))
	}
	odb2.Close()
	OrderFileClear(odb2path)
}

func TestSpecKey1(t *testing.T) {
	orderpath := "testdb/TestSpecKey1"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	var testdb *OrderFile
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)

	testdb.RealPush([]byte("aabbcc"))

	fmt.Println(testdb.FillKey([]byte("aabbc")))

	testdb.RealPush([]byte("aabbdd"))
	testdb.RealPush([]byte("aabb"))
	if testdb.Count() != 3 {
		panic("count error!")
	}
	curkey := []byte{}
	for true {
		nextkey, bok := testdb.NextKey(curkey)
		if !bok {
			break
		}
		fmt.Println(string(nextkey))
		curkey = nextkey
	}

	fmt.Println("fill key")
	_, bfillok := testdb.FillKey([]byte("aabb"))
	if !bfillok {
		panic("remove error.")
	}

	bok := testdb.RealRm([]byte("aabb"))
	if bok == false {
		panic("remove error!")
	}
	if testdb.Count() != 2 {
		panic("count error.")
	}

	curkey = []byte{}
	for true {
		nextkey, bok := testdb.NextKey(curkey)
		if !bok {
			break
		}
		fmt.Println(string(nextkey))
		curkey = nextkey
	}

	testdb.Close()
	OrderFileClear(orderpath)
}

func TestDeleteToEmptyError(t *testing.T) {
	orderpath := "testdb/DeleteToEmptyError"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	testdb, _ := OpenOrderFile(orderpath, 0, 0, nil)
	testdb.RealPush([]byte("http://sc.cri.cn/20190203/88444745-9a63-e7dc-9203-6ce2eef06774.html"))
	testdb.RealPush([]byte("http://sc.cri.cn/20190203/1756b803-2d06-8f16-ebe3-a7d277343b36.html"))
	testdb.RealRm([]byte("http://sc.cri.cn/20190203/88444745-9a63-e7dc-9203-6ce2eef06774.html"))
	testdb.RealRm([]byte("http://sc.cri.cn/20190203/1756b803-2d06-8f16-ebe3-a7d277343b36.html"))

	testdb.Flush()
	testdb.Close()
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	testdb.Close()
	OrderFileClear(orderpath)
}

func TestNewsReopenError(t *testing.T) {
	orderpath := "testdb/NewsReopenError"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	testdb, _ := OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	time.Sleep(1 * time.Second)
	testdb.PushKey([]byte("http://sc.cri.cn/20180821/5214ce44-1b57-c896-f787-b11c24a5a790.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e452aacc-861f-24e0-9ea2-b8171463b890.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e4134430-fd3d-fa5c-ba3d-8ed2165ff671.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180927/926bda3b-7652-9bc3-b935-a9dd965cfb8e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180815/45a95277-367c-3bb4-e6df-67433b894caf.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181015/65b985fe-3c96-acec-3bbc-730ddf2c5ec8.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181119/ea692cb9-b26b-e502-72e4-e9d47c2613f1.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190107/fb4abf5d-8129-6be5-44f6-195d66929e8c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/44e92df2-5b1c-aa96-d19c-234ab01f252a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/eba6b60a-ecf2-57ac-c4b9-b1e66923bb72.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190125/a0bcf08d-3b82-3bb5-c1dd-f585aea5afc6.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/4e0910bd-26d1-a319-c377-3d5ab327b944.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190109/f8423905-9d2c-6dd0-ed1e-f2d652272017.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/324339c3-bf82-45ba-b937-c6caa7267a6b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/b90c6090-ebf9-985a-5dee-3f22c61af96a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/8065cbb2-a204-3f88-3690-f4b48460ca7e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190203/e0cb6421-745d-2db2-e9d5-2e99756a9a14.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/8fd0ad95-f7b1-366a-b153-56e85f0d2293.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/8160c5bf-e5c7-9f8e-716b-6b5f3378bc06.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/352c3ac1-28e1-5a03-aa07-bab22560b182.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180723/16a4e4c9-899d-b7a2-59a8-47dd37a5ba7e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180725/bf96f2c8-4a83-d107-1e93-cefee1a5c957.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180808/a71dafc1-bbe6-e31d-b0c4-4ed17b0c1939.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180813/8c6f4f73-ec10-c964-10ec-f35accfabce6.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180815/7b60cb0b-902c-96eb-52b3-ffc33b098aab.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180823/aee3a3b6-b167-4b78-677f-612cce1e1310.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180824/692c3bb8-a697-fe9f-dd58-ebb3e3585843.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180829/8b5cad0f-fc69-775d-53d4-8ec76c4f6429.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/7b9e5137-2408-1c48-2a2e-159025d6c876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e56ec5d8-3a8a-915e-a3f7-17a3c332ef86.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180906/cedba55c-bb1f-47ed-8317-e4d93d70c78c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180912/4b367895-78b4-f168-63b7-8956ab9f7e6b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180914/7450ea0e-8f7b-1089-59c0-df09b472ad32.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180917/ba2b74df-2807-9bd2-7a08-30f57be487c8.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180921/8530415b-1a04-5b5a-4625-26122f7bfc98.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180925/c378f146-a0a8-ad55-9c0c-d7b1651d9a4b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180926/c2c67246-115a-a6ed-4877-2c4948e39310.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180928/263d78f8-8cf3-b9fb-bcbf-d514f608bc13.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180930/ea6cc3e4-dc7b-f95e-717d-e989d956a6a4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181008/c7917509-7cee-1a8d-e849-fc4fa718aa27.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181009/c53de50c-7fd9-f0de-a524-1b3a6d8fe981.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181017/fc14d8f3-9ad8-10f0-490f-1cd7b49f630c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181019/77e2defd-ddff-1f19-2d3b-bd2693d94272.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181022/1d649b47-8dbf-ca1e-39f4-d4f817187bea.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181025/8396f2d2-aaf0-eece-68bb-1a3a5878f7ae.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181030/3fc9a216-8435-d549-e875-58de574639cd.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181031/8830805e-2266-63b8-1f55-34222ba45e13.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181101/595f8b6c-923d-b561-257a-71dcab26d6ac.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181105/c9b7b0f2-b939-ca9d-12ee-f399319ccf51.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181107/76dcf3f0-aaa1-c81b-961c-a7d7b8d8e4b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181112/d375d4fe-586b-51c2-504d-e8b69cd0dea4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181116/2e32a7f4-bd45-3793-44ed-c05c1b9e845c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181119/830936f9-7ff2-74d2-adc6-415f80ecb266.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181129/e670d5b2-3225-d1e4-21e1-3a812565f14d.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181204/ec32822f-43ad-f24d-c400-d317477a7220.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181218/76169686-b905-5d09-4cfe-9b7b121d7c00.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181219/56013f44-d691-3877-9a36-6a6c1db142ae.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181226/ef5d4ec3-b61d-76f4-7f75-6132c1955176.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190107/9d6cb5b5-9b35-5497-ecc2-cbd07985a19c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/43d6f471-db75-b743-4cd8-d94ad7d4fa1d.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190121/6a41173d-2563-b2a8-81d1-9dfae23b3c18.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190122/128a479e-549e-f806-b99c-4a5117be4859.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190128/11e8dc66-5a23-b33a-1966-36761d3f9cf5.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190129/330f8a52-910b-7b78-a922-e196f52e765b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/1dcd28be-5279-9709-2abf-de9dd5fe5de6.html"), []byte{})
	testdb.RmKey([]byte("http://sc.cri.cn/highlights.html"))
	time.Sleep(1 * time.Second)
	testdb.Close()
	time.Sleep(1 * time.Second)
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	time.Sleep(1 * time.Second)
	testdb.PushKey([]byte("http://sc.cri.cn/20180821/5214ce44-1b57-c896-f787-b11c24a5a790.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e452aacc-861f-24e0-9ea2-b8171463b890.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e4134430-fd3d-fa5c-ba3d-8ed2165ff671.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180927/926bda3b-7652-9bc3-b935-a9dd965cfb8e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180815/45a95277-367c-3bb4-e6df-67433b894caf.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181015/65b985fe-3c96-acec-3bbc-730ddf2c5ec8.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181119/ea692cb9-b26b-e502-72e4-e9d47c2613f1.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190107/fb4abf5d-8129-6be5-44f6-195d66929e8c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/44e92df2-5b1c-aa96-d19c-234ab01f252a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/eba6b60a-ecf2-57ac-c4b9-b1e66923bb72.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190125/a0bcf08d-3b82-3bb5-c1dd-f585aea5afc6.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/4e0910bd-26d1-a319-c377-3d5ab327b944.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190109/f8423905-9d2c-6dd0-ed1e-f2d652272017.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/324339c3-bf82-45ba-b937-c6caa7267a6b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/b90c6090-ebf9-985a-5dee-3f22c61af96a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/8065cbb2-a204-3f88-3690-f4b48460ca7e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/8fd0ad95-f7b1-366a-b153-56e85f0d2293.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/8160c5bf-e5c7-9f8e-716b-6b5f3378bc06.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/352c3ac1-28e1-5a03-aa07-bab22560b182.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6a6a7224-357d-93b5-c9cc-c71d12f567e5.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190203/88444745-9a63-e7dc-9203-6ce2eef06774.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190203/1756b803-2d06-8f16-ebe3-a7d277343b36.html"), []byte{})
	testdb.RmKey([]byte("http://sc.cri.cn/20190203/e0cb6421-745d-2db2-e9d5-2e99756a9a14.html"))
	time.Sleep(1 * time.Second)
	testdb.Close()
	time.Sleep(1 * time.Second)
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	testdb.PushKey([]byte("http://sc.cri.cn/20180821/5214ce44-1b57-c896-f787-b11c24a5a790.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e452aacc-861f-24e0-9ea2-b8171463b890.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e4134430-fd3d-fa5c-ba3d-8ed2165ff671.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180927/926bda3b-7652-9bc3-b935-a9dd965cfb8e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180815/45a95277-367c-3bb4-e6df-67433b894caf.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181015/65b985fe-3c96-acec-3bbc-730ddf2c5ec8.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181119/ea692cb9-b26b-e502-72e4-e9d47c2613f1.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190107/fb4abf5d-8129-6be5-44f6-195d66929e8c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/44e92df2-5b1c-aa96-d19c-234ab01f252a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/eba6b60a-ecf2-57ac-c4b9-b1e66923bb72.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190125/a0bcf08d-3b82-3bb5-c1dd-f585aea5afc6.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/4e0910bd-26d1-a319-c377-3d5ab327b944.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190109/f8423905-9d2c-6dd0-ed1e-f2d652272017.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/324339c3-bf82-45ba-b937-c6caa7267a6b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/b90c6090-ebf9-985a-5dee-3f22c61af96a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/8065cbb2-a204-3f88-3690-f4b48460ca7e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/8fd0ad95-f7b1-366a-b153-56e85f0d2293.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/8160c5bf-e5c7-9f8e-716b-6b5f3378bc06.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/352c3ac1-28e1-5a03-aa07-bab22560b182.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/9abceede-a248-a083-2017-8b655b9ec969.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6a6a7224-357d-93b5-c9cc-c71d12f567e5.html"), []byte{})
	time.Sleep(1 * time.Second)
	testdb.RmKey([]byte("http://sc.cri.cn/20190203/88444745-9a63-e7dc-9203-6ce2eef06774.html"))
	time.Sleep(1 * time.Second)
	testdb.Close()
	time.Sleep(1 * time.Second)
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	testdb.PushKey([]byte("http://sc.cri.cn/20180821/5214ce44-1b57-c896-f787-b11c24a5a790.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e452aacc-861f-24e0-9ea2-b8171463b890.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180831/e4134430-fd3d-fa5c-ba3d-8ed2165ff671.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180927/926bda3b-7652-9bc3-b935-a9dd965cfb8e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20180815/45a95277-367c-3bb4-e6df-67433b894caf.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181015/65b985fe-3c96-acec-3bbc-730ddf2c5ec8.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181119/ea692cb9-b26b-e502-72e4-e9d47c2613f1.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190107/fb4abf5d-8129-6be5-44f6-195d66929e8c.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/44e92df2-5b1c-aa96-d19c-234ab01f252a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/eba6b60a-ecf2-57ac-c4b9-b1e66923bb72.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190125/a0bcf08d-3b82-3bb5-c1dd-f585aea5afc6.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/4e0910bd-26d1-a319-c377-3d5ab327b944.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190123/84b5fdba-554d-1177-157e-2d7c8d5290b4.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190109/f8423905-9d2c-6dd0-ed1e-f2d652272017.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190118/324339c3-bf82-45ba-b937-c6caa7267a6b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/b90c6090-ebf9-985a-5dee-3f22c61af96a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/8065cbb2-a204-3f88-3690-f4b48460ca7e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20181208/d30b213a-a954-e40c-c78c-876845b8a96e.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190131/8fd0ad95-f7b1-366a-b153-56e85f0d2293.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/8160c5bf-e5c7-9f8e-716b-6b5f3378bc06.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190201/352c3ac1-28e1-5a03-aa07-bab22560b182.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6cdc8198-c8d7-fcd9-df8c-98fd9b9b807a.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/bdfb7fea-beed-6f48-4387-2ce15748b14b.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/9abceede-a248-a083-2017-8b655b9ec969.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/7065dd18-f076-aa98-d033-bcaaefc15876.html"), []byte{})
	testdb.PushKey([]byte("http://sc.cri.cn/20190202/6a6a7224-357d-93b5-c9cc-c71d12f567e5.html"), []byte{})
	testdb.RmKey([]byte("http://sc.cri.cn/20190203/1756b803-2d06-8f16-ebe3-a7d277343b36.html"))
	time.Sleep(1 * time.Second)
	testdb.Close()
	time.Sleep(1 * time.Second)
	testdb, _ = OpenOrderFile(orderpath, 0, 0, nil)
	testdb.PreviousKey([]byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"))
	testdb.Close()
	OrderFileClear(orderpath)
}

func TestOrderFileNextKey(t *testing.T) {
	orderpath := "testdb/TestOrderFileNextKey"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	endbt := []byte{0}
	testof, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword2 := []string{}
	allword2map := make(map[string]string, 0)
	//nextkeytestf, _ := os.Create("nextkeytest.txt")
	for i := 0; i < 10000; i++ {
		word2 := genword()
		word2 = append(word2, byte(0))
		word3 := genword()
		//word2 := genfloat32bt()
		_, be := allword2map[string(word2)]
		if !be {
			testof.PushKey(word2, word3)
			fillkey, bfillok := testof.FillKey(word2)
			if string(fillkey) != string(word2)+string("\x00")+string(word3) {
				fmt.Println("error ", string(word2), "word3：", string(word3), "fillkey", string(fillkey), bfillok)
				panic("error")
			}
			//fmt.Println(string(word2))
			allword2 = append(allword2, string(word2))
			//nextkeytestf.Write(word2)
			//nextkeytestf.Write([]byte("\n"))
			allword2map[string(word2)] = string(word3)
			time.Sleep(15 * time.Microsecond)
		}
	}
	testof.WaitBufMapEmpty()
	//nextkeytestf.Close()
	fmt.Println("next totalcnt:", len(allword2map))

	sort.Strings(allword2)
	if len(allword2) < 100 {
		fmt.Println("allword2:", allword2)
	}
	nextkey2, _ := testof.NextKey([]byte(""))
	fmt.Println("nextkey2:", string(nextkey2))

	for key, val := range allword2map {
		//fmt.Println("next key :", key)
		nextkey, bfound := testof.NextKey([]byte(key))
		if bfound == false || key+val != string(nextkey) {
			_, bfill := testof.FillKey([]byte(key))
			if bfill == false {
				fillkey, bfound2 := testof.FillKey([]byte(key))
				fillkey2, bfound3 := testof.FillKey([]byte(key + val))
				fmt.Println("key val", key, val, "found nextkey:", string(nextkey), "fillkey:", string(fillkey), bfound2, string(fillkey2), bfound3)
				panic("next key error!")
			}
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKey(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKey"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	//seed = 1554697438
	rand.Seed(seed)

	testof, _ := OpenOrderFile(orderpath, 0, 0, nil)
	allword2 := []string{}
	allword2map := make(map[string]string, 0)
	//nextkeytestf, _ := os.Create("previouskeytest.txt")
	for i := 0; i < 10000; i++ {
		word2 := genword()
		word2 = append(word2, byte(0))
		word3 := genword()
		_, be := allword2map[string(word2)]
		if !be {
			testof.PushKey(word2, word3)
			//fmt.Println(string(word2))
			allword2 = append(allword2, string(word2))
			//nextkeytestf.Write(word2)
			//nextkeytestf.Write([]byte("\n"))
			//nextkeytestf.Sync()
			allword2map[string(word2)] = string(word3)
			time.Sleep(15 * time.Microsecond)
		}
	}
	testof.WaitBufMapEmpty()
	testof.Close()
	sort.Strings(allword2)
	testof, _ = OpenOrderFile(orderpath, 0, 0, nil)
	for key, val := range allword2map {
		//fmt.Println("find previous for :", allword2[i])
		nextkey, bfound := testof.PreviousKey([]byte(key))
		bfnd := false
		for ind, _ := range allword2 {
			if ind > 0 && allword2[ind] == key {
				if allword2[ind-1] != string([]byte(nextkey)[:len([]byte(allword2[ind-1]))]) {
					fmt.Println("key val", key, "val:", val, "found:", string(nextkey), bfound, " should:", allword2[ind-1], " getpart:", string([]byte(nextkey)[:len([]byte(key))]))
					panic("previous key error!")
				}
				bfnd = true
				break
			}
		}
		if bfnd == false && key != allword2[0] {
			_, bfill := testof.FillKey([]byte(key))
			if bfill == false {
				fmt.Println("key val", key, "val:", val, "found:", string(nextkey), bfound)
				panic("error")
			}
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile18(t *testing.T) {
	orderpath := "testdb/TestOrderFile18"
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".compress")
	seed := time.Now().Unix()
	//seed = 1549096459
	//seed = 1549108913
	//seed = 1549109180
	//seed = 1549110151
	//seed = 1553192386
	//seed = 1555758009
	//seed = 1630844980
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	testof, _ := OpenOrderFile(orderpath, 0, 0, []byte{0})
	allword3 := make(map[string]string, 0)
	//msec := time.Duration(50)
	for i := 0; i < 10000; i++ {
		word := genword() //genwordlarge()
		word2 := genword()
		_, isword := allword3[string(word2)]
		if !isword {
			testof.PushKey(word2, word)
			allword3[string(word2)] = string(word)
			// time.Sleep(msec * time.Microsecond)
			// msec -= 5
		}
	}

	// of, _ := os.OpenFile("orderfile_rare_error_fill_no_found_wordset1.txt", os.O_RDWR, 0666)
	// rd := bufio.NewReader(of)
	// for true {
	// 	str, err := rd.ReadString('\n')
	// 	if err != nil {
	// 		break
	// 	}
	// 	strls := strings.Split(str, " ")
	// 	key := append([]byte(strls[0]), byte(0))
	// 	testof.PushKey(key, []byte(strls[1]))
	// 	allword3[string(key)] = string(strls[1])
	// 	time.Sleep(20 * time.Microsecond)
	// }

	testof.Close()

	fmt.Println("Init count:", len(allword3))
	DBCompress(orderpath, false, 10)
	testof, _ = OpenOrderFile(orderpath+".compress", 0, 0, []byte{0})
	for word3, word2 := range allword3 {
		//fmt.Println(string(word3))
		fullword, _ := testof.FillKey([]byte(word3))
		if string(fullword) != word3+string("\x00")+word2 {
			fmt.Println("out all")
			fmt.Println("FillKey:", string(fullword))
			fmt.Println("FillKey byte:", fullword)
			fmt.Println("Right Key:", word3+string("\x00")+word2)
			fmt.Println("Right Key byte:", []byte(word3+string("\x00")+word2))
			curkey := ""
			curkeycnt := 0
			for true {
				fullword, bgetcurkey := testof.NextKey([]byte(curkey))
				if bgetcurkey {
					//fmt.Println("fullword:", string(fullword))
					curkeycnt += 1
					curkey = string(fullword)
				} else {
					break
				}
			}
			fmt.Println("total out:", curkeycnt, len(allword3))
			fmt.Println("error", word3+word2)
			panic("error")
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".compress")
}

func TestOrderFile19(t *testing.T) {
	orderpath := "testdb/TestOrderFile19"
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".new")
	seed := time.Now().Unix()
	//seed = 1549096459
	//seed = 1549108913
	//seed = 1549109180
	//seed = 1549110151
	//seed = 1550412126
	//seed = 1554701950
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	fmt.Println("start neworder push")
	allword2 := make(map[string]string, 0)
	testof, _ := OpenOrderFile(orderpath+".new", 0, 0, nil)
	wordset2 := []string{}
	msec := time.Duration(50)
	for i := 0; i < 10000; i++ {
		word := genwordlarge()
		word2 := genword()
		word2 = append(word2, byte(0))
		_, isword := allword2[string(word2)]
		if !isword {
			wordset2 = append(wordset2, string(word2[:len(word2)-1])+" "+string(word))
			//bigwordtestf.Write([]byte(string(word2) + "\t" + string(word)))
			//bigwordtestf.Write([]byte("\n"))
			//bigwordtestf.Sync()
			testof.PushKey(word2, word)
			allword2[string(word2)] = string(word)
			//fmt.Println("testget")
			time.Sleep(msec * time.Millisecond)
			msec -= 5
		}
	}

	// of2, _ := os.OpenFile("orderfile_rare_error_fill_no_found_wordset1.txt", os.O_RDWR, 0666)
	// rd2 := bufio.NewReader(of2)
	// for true {
	// 	str, err := rd2.ReadString('\n')
	// 	if err != nil {
	// 		break
	// 	}
	// 	strls := strings.Split(str, " ")
	// 	key := append([]byte(strls[0]), byte(0))
	// 	testof.PushKey(key, []byte(strls[1]))
	// 	allword2[string(key)] = string(strls[1])
	// 	//fmt.Println("testget")
	// 	time.Sleep(15 * time.Microsecond)
	// }

	testof.Close()
	fmt.Println("end2")
	testof, _ = OpenOrderFile(orderpath+".new", 0, 0, nil)
	for word3, word2 := range allword2 {
		fullword, fillkeyok := testof.FillKey([]byte(word3))
		if string(fullword) != word3+word2 {
			time.Sleep(1 * time.Second)
			fullword2, fillkeyok2 := testof.FillKey([]byte(word3))
			nextkey2, nextkey2ok := testof.NextKey([]byte(word3))
			fmt.Println("error", word3+word2)
			fmt.Println(string(fullword), "fillkeyok", fillkeyok)
			fmt.Println("fullword2, fillkeyok2", string(fullword2), fillkeyok2)
			fmt.Println("nextkey2, nextkey2ok", string(nextkey2), nextkey2ok)
			word2f, _ := os.Create("wordset2.txt")
			for _, word := range wordset2 {
				word2f.Write([]byte(word))
				word2f.Write([]byte("\n"))
			}
			word2f.Close()
			fmt.Println(runtime.Caller(0))
			panic("error")
		}
	}
	testof.Close()

	//3 time reload check
	fmt.Println("3 time reload check")
	testof, _ = OpenOrderFile(orderpath+".new", 0, 0, nil)
	for i := 0; i < 10000; i++ {
		word := genwordlarge()
		word2 := genword()
		word2 = append(word2, byte(0))
		_, isword := allword2[string(word2)]
		if !isword {
			//bigwordtestf.Write([]byte(string(word2) + "\t" + string(word)))
			//bigwordtestf.Write([]byte("\n"))
			//bigwordtestf.Sync()
			//fmt.Println("word1:", word, string(word))
			//fmt.Println("word2:", word2, string(word2))
			//fmt.Println(string(word2))
			testof.PushKey(word2, word)
			allword2[string(word2)] = string(word)
			time.Sleep(15 * time.Microsecond)
		}
	}

	fmt.Println("orderfile 3time fillkey")

	for word3, word2 := range allword2 {
		fullword, _ := testof.FillKey([]byte(word3))
		if string(fullword) != word3+word2 {
			fmt.Println("error", word3+word2)
			fmt.Println("found:", string(fullword))
			fmt.Println(runtime.Caller(0))
			panic("error")
		}
	}

	dbcount := testof.Count()
	fmt.Println("dbcount:", dbcount)
	testof.Close()
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".new")
}

func TestHoldZeroLengthValue(t *testing.T) {
	orderpath := "testdb/TestHoldZeroLengthValue"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof, _ := OpenOrderFile(orderpath, 0, 0, nil)
	//testof.PushKey([]byte(""))
	testof.PushKey([]byte("iodsifj"), []byte{})
	testof.PushKey([]byte("ioiodfmfm"), []byte{})
	testof.PushKey([]byte("dkljksdfljsg"), []byte{})
	testof.PushKey([]byte(""), []byte{})
	testof.PushKey([]byte("dsdklfjd"), []byte{})
	testof.PushKey([]byte("dkiiodfj"), []byte{})

	result, bresult := testof.FillKey([]byte(""))
	fmt.Println("result", result, bresult)

	curkey := []byte{0xff, 0xff, 0xff, 0xff}
	for true {
		prekey, bprekey := testof.PreviousKey(curkey)
		if !bprekey {
			break
		}
		fmt.Println(string(prekey), bprekey)
		curkey = prekey
	}
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFileKKK3NextError(t *testing.T) {
	orderpath := "testdb/TestOrderFileKKK3NextError"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof4, _ := OpenOrderFile(orderpath, 0, 0, nil)
	//prekey := []byte("")
	prekey := []byte("kkk3_") //will get error next key
	for true {
		prekey, _ = testof4.NextKey(prekey)
		if len(prekey) == 0 {
			break
		}
		fmt.Println(string(prekey[:bytes.Index(prekey, []byte{0})]), prekey)
		if "kkk3_0450954" != string(prekey[:bytes.Index(prekey, []byte{0})]) {
			panic("error!")
		}
		break
	}

	prekey = []byte("op0zsdc")
	for true {
		prekey, _ = testof4.PreviousKey(prekey)
		if len(prekey) == 0 {
			break
		}
		fmt.Println(string(prekey[:bytes.Index(prekey, []byte{0})]), prekey)
		if "kkk_vallist" != string(prekey[:bytes.Index(prekey, []byte{0})]) {
			panic("error!")
		}
		break
	}

	prekey = []byte("wwww")
	for true {
		prekey, _ = testof4.PreviousKey(prekey)
		if len(prekey) == 0 {
			break
		}
		fmt.Println(string(prekey[:bytes.Index(prekey, []byte{0})]), prekey)
		if "wwksif_vallist" != string(prekey[:bytes.Index(prekey, []byte{0})]) {
			panic("error!")
		}
		break
	}

	prekey = []byte("kkk_dsafds")
	for true {
		prekey, _ = testof4.PreviousKey(prekey)
		if len(prekey) == 0 {
			break
		}
		fmt.Println(string(prekey[:bytes.Index(prekey, []byte{0})]), prekey)
		if "kkk4_vallist" != string(prekey[:bytes.Index(prekey, []byte{0})]) {
			panic("error!")
		}
		break
	}
	testof4.Close()
	OrderFileClear(orderpath)
}

func TestOrderFileNextKeytestsample6(t *testing.T) {
	orderpath := "testdb/TestOrderFileNextKeytestsample6"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	endbt := []byte{0}
	//endbt := []byte{}
	testof2, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword3 := []string{}
	bigword, _ := ioutil.ReadFile("testsample6.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		testof2.PushKey([]byte(lines[i]), []byte{})
		allword3 = append(allword3, string(lines[i]))
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	// testof2.markrmpushmap.Range(func(key, val interface{}) bool {
	// 	fmt.Println(key, val)
	// 	return true
	// })
	sort.Strings(allword3)
	fmt.Println(allword3)
	for i := 0; i < len(allword3); i++ {
		fmt.Println("find next key for:", allword3[i])
		nextkey, bfound := testof2.NextKey(append([]byte(allword3[i]), endbt...))
		if i < len(allword3)-1 {
			if bfound == false || allword3[i+1]+string(endbt) != string(nextkey) {
				// testof2.markrmpushmap.Range(func(key, val interface{}) bool {
				// 	fmt.Println(key, val)
				// 	return true
				// })
				fmt.Println("i", i, "currentkey:", allword3[i], "found nextkey:", string(nextkey), "right nextkey:", allword3[i+1])
				fmt.Println("found nextkey byte:", "found nextkey:", nextkey)
				fmt.Println("right nextkey byte:", []byte(allword3[i+1]))
				panic("next key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeytestsample5(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKeytestsample5"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof2, _ := OpenOrderFile(orderpath, 0, 0, nil)
	allword3 := []string{}
	allword3map := make(map[string]int, 0)
	bigword, _ := ioutil.ReadFile("testsample5.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		_, be := allword3map[string(lines[i])]
		if !be {
			testof2.PushKey([]byte(lines[i]), []byte{})
			allword3 = append(allword3, string(lines[i]))
			allword3map[string(lines[i])] = 1
		}
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	sort.Strings(allword3)
	if len(allword3) < 100 {
		fmt.Println("allword3:", allword3)
	}
	for i := len(allword3) - 1; i >= 0; i-- {
		fmt.Println("find previous for :", allword3[i])
		nextkey, bfound := testof2.PreviousKey([]byte(allword3[i]))
		if i > 0 {
			if bfound == false || allword3[i-1] != string(nextkey) {
				fmt.Println("i", i, "currentkey", allword3[i], "found nextkey:", string(nextkey), "right nextkey:", allword3[i-1])
				panic("next key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeytestsample4(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKeytestsample4"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	endbt := []byte{'\t'}
	testof2, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword3 := []string{}
	allword3map := make(map[string]int, 0)
	bigword, _ := ioutil.ReadFile("testsample4.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		_, be := allword3map[string(lines[i])]
		if !be {
			testof2.PushKey([]byte(lines[i]), []byte{})
			allword3 = append(allword3, string(lines[i])+"\t")
			allword3map[string(lines[i])] = 1
		}
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	// testof2.markrmpushmap.Range(func(key, val interface{}) bool {
	// 	fmt.Println(key, val)
	// 	return true
	// })
	sort.Strings(allword3)
	if len(allword3) < 100 {
		fmt.Println("allword3:", allword3)
	}
	for i := len(allword3) - 1; i >= 0; i-- {
		fmt.Println("find previous for :", allword3[i])
		nextkey, bfound := testof2.PreviousKey([]byte(allword3[i]))
		if i > 0 {
			if bfound == false || allword3[i-1] != string(nextkey) {
				// testof2.markrmpushmap.Range(func(key, val interface{}) bool {
				// 	fmt.Println(key, val)
				// 	return true
				// })
				fmt.Println("i", i, "currentkey", allword3[i], "found previouskey:", string(nextkey), "right previouskey:", allword3[i-1])
				panic("previous key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeytestsample3(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKeytestsample3"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof2, _ := OpenOrderFile(orderpath, 0, 0, nil)
	allword3 := []string{}
	allword3map := make(map[string]int, 0)
	bigword, _ := ioutil.ReadFile("testsample3.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		_, be := allword3map[string(lines[i])]
		if !be {
			testof2.PushKey([]byte(lines[i]), []byte{})
			allword3 = append(allword3, string(lines[i]))
			allword3map[string(lines[i])] = 1
		}
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	sort.Strings(allword3)
	if len(allword3) < 100 {
		fmt.Println("allword3:", allword3)
	}
	for i := len(allword3) - 1; i >= 0; i-- {
		fmt.Println("find previous for :", allword3[i])
		nextkey, bfound := testof2.PreviousKey([]byte(allword3[i]))
		if i > 0 {
			if bfound == false || allword3[i-1] != string(nextkey) {
				fmt.Println("i", i, "currentkey", allword3[i], "found nextkey:", string(nextkey), "right nextkey:", allword3[i-1])
				panic("next key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeyTestSample2(t *testing.T) {
	orderpath := "TestOrderFilePreviousKeyTestSample2"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	endbt := []byte{0}
	testof2, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword3 := []string{}
	bigword, _ := ioutil.ReadFile("testsample2.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		testof2.PushKey([]byte(lines[i]), []byte{})
		allword3 = append(allword3, string(lines[i]))
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	sort.Strings(allword3)
	if len(allword3) < 100 {
		fmt.Println("allword3:", allword3)
	}
	for i := len(allword3) - 1; i >= 0; i-- {
		fmt.Println("find previous for :", allword3[i])
		nextkey, bfound := testof2.PreviousKey([]byte(allword3[i]))
		if i > 0 {
			if bfound == false || allword3[i-1]+string(endbt) != string(nextkey) {
				fmt.Println("i", i, "found nextkey:", string(nextkey), "right nextkey:", allword3[i-1]+string(endbt))
				panic("next key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFilePreviousKeyTestSample1(t *testing.T) {
	orderpath := "testdb/TestOrderFilePreviousKeyTestSample1"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof2, _ := OpenOrderFile(orderpath, 0, 0, nil)
	allword3 := []string{}
	bigword, _ := ioutil.ReadFile("testsample1.txt")
	lines := strings.Split(string(bigword), "\n")
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			continue
		}
		testof2.PushKey([]byte(lines[i]), []byte{})
		allword3 = append(allword3, string(lines[i]))
	}
	testof2.WaitBufMapEmpty()
	testof2.Flush()
	sort.Strings(allword3)
	for i := len(allword3) - 1; i >= 0; i-- {
		fmt.Println("find previous for :", allword3[i])
		nextkey, bfound := testof2.PreviousKey([]byte(allword3[i]))
		if i > 0 {
			if bfound == false || allword3[i-1] != string(nextkey) {
				fmt.Println("i", i, "found nextkey:", string(nextkey), "right nextkey:", allword3[i-1])
				panic("next key error!")
			}
		} else {
			if bfound == true {
				panic("end next is not valid!")
			}
		}
	}
	testof2.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile12(t *testing.T) {
	orderpath := "testdb/TestOrderFile12"
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".compress")
	seed := time.Now().Unix()
	fmt.Println("seed:", seed)
	rand.Seed(seed)

	testof, _ := OpenOrderFile(orderpath, 0, 0, []byte{0})
	allword2 := make(map[string][]byte, 0)
	for i := 0; i < 50000; i++ {
		word := genwordlarge()
		word2 := genword()
		_, isword := allword2[string(word2)]
		if !isword {
			testof.PushKey(word2, word)
			allword2[string(word2)] = word
		}
	}
	testof.Flush()
	testof.Close()

	DBCompress(orderpath, false, 10)

	testof, _ = OpenOrderFile(orderpath+".compress", 0, 0, []byte{0})
	for word3, word2 := range allword2 {
		fullword, bfill := testof.FillKey([]byte(word3))
		if bfill == false || string(fullword) != word3+"\x00"+string(word2) {
			fmt.Println("error", word3+string(word2))
			fmt.Println(string(fullword))
			fmt.Println(runtime.Caller(0))
			panic("error")
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
	OrderFileClear(orderpath + ".compress")
}

func TestOrderFile17(t *testing.T) {
	orderpath := "testdb/TestOrderFile17"
	OrderFileClear(orderpath)

	testof, _ := OpenOrderFile(orderpath, 0, 0, nil)

	testof.RmKey([]byte{0, 0, 0, 45, 0})
	testof.Push([]byte{0, 0, 0, 45, 0, 100, 97, 116, 97, 112, 97, 116, 104, 9, 116, 101, 115, 116, 119, 101, 98, 115, 105, 116, 101, 47, 116, 101, 99, 104, 46, 105, 102, 101, 110, 103, 46, 99, 111, 109, 10, 100, 97, 116, 97, 105, 100, 9, 49})
	testof.RmKey([]byte{0, 0, 0, 68, 0})
	testof.Push([]byte{0, 0, 0, 68, 0, 100, 97, 116, 97, 112, 97, 116, 104, 9, 116, 101, 115, 116, 119, 101, 98, 115, 105, 116, 101, 47, 116, 101, 99, 104, 46, 105, 102, 101, 110, 103, 46, 99, 111, 109, 10, 100, 97, 116, 97, 105, 100, 9, 50})
	testof.RmKey([]byte{0, 0, 0, 101, 0})
	testof.Push([]byte{0, 0, 0, 101, 0, 100, 97, 116, 97, 112, 97, 116, 104, 9, 116, 101, 115, 116, 119, 101, 98, 115, 105, 116, 101, 47, 116, 101, 99, 104, 46, 105, 102, 101, 110, 103, 46, 99, 111, 109, 10, 100, 97, 116, 97, 105, 100, 9, 50})
	testof.RmKey([]byte{0, 0, 0, 33, 0})
	testof.Push([]byte{0, 0, 0, 33, 0, 100, 97, 116, 97, 112, 97, 116, 104, 9, 116, 101, 115, 116, 119, 101, 98, 115, 105, 116, 101, 47, 116, 101, 99, 104, 46, 105, 102, 101, 110, 103, 46, 99, 111, 109, 10, 100, 97, 116, 97, 105, 100, 9, 49})
	testof.RmKey([]byte{0, 0, 0, 50, 0})
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile24(t *testing.T) {
	orderpath := "testdb/TestOrderFile24"
	OrderFileClear(orderpath)
	rand.Seed(time.Now().Unix())
	endbt := []byte{0}
	testof, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword2 := make(map[string]string, 0)
	for i := 0; i < 50000; i++ {
		//word := genword() //genwordlarge()
		word2 := genword()
		testof.PushKey(word2, word2)

		allword2[string(word2)] = string(word2)
		fullword, bfill := testof.FillKey(word2)
		if bfill == false || string(fullword) != string(word2)+string(endbt)+string(word2) {
			fmt.Println("error", i)
			fmt.Println("right byte:", []byte(string(word2)+string(endbt)+string(word2)))
			fmt.Println("get byte:", fullword)
			panic("error")
		}
	}
	testof.Close()
	//bigwordtestf.Close()

	fmt.Println("reload test")
	testof, _ = OpenOrderFile(orderpath, 0, 0, endbt)
	for key, val := range allword2 {
		fullword, bfill := testof.FillKey([]byte(key))
		if bfill == false || string(fullword) != string(key)+string(endbt)+string(val) {
			fmt.Println("error", string(key)+string(endbt)+string(val), string(fullword))
			fmt.Println("right byte:", []byte(string(key)+string(endbt)+string(val)))
			fmt.Println("get byte:", fullword)
			panic("error")
		}
	}
	fmt.Println("remove test")
	for key, _ := range allword2 {
		rmrl := testof.RmKey([]byte(key))
		if !rmrl {
			fmt.Println("error")
			panic("error")
		}
	}

	for key, _ := range allword2 {
		fullword, bfill := testof.FillKey([]byte(key))
		if bfill == true {
			fmt.Println("error", bfill, fullword)
			panic("error")
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile14(t *testing.T) {
	orderpath := "testdb/TestOrderFile14"
	OrderFileClear(orderpath)
	rand.Seed(time.Now().Unix())

	endbt := []byte{'\t'}
	testof, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword2 := make(map[string][]byte, 0)
	for i := 0; i < 5000; i++ {
		word := genwordlarge()
		word2 := genword()
		_, isword := allword2[string(word2)]
		if !isword {
			_, err := allword2[string(word2)]
			if !err {
				testof.PushKey(word2, word)
				allword2[string(word2)] = word

				fullword, bfill := testof.FillKey(word2)
				if bfill == false || string(fullword) != string(word2)+"\t"+string(word) {
					fmt.Println("error", string(word2)+"\t"+string(word), string(fullword))
					fmt.Println(runtime.Caller(0))
					panic("error")
				}
			}
		}
	}
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile10(t *testing.T) {
	orderpath := "testdb/TestOrderFile10"
	OrderFileClear(orderpath)
	seed := time.Now().Unix()
	//seed = 1632031943
	fmt.Println("seed:", seed)
	rand.Seed(seed)
	endbt := []byte{'\t'}
	testof, _ := OpenOrderFile(orderpath, 0, 0, endbt)
	allword2 := make(map[string][]byte, 0)
	bigwordtestf, _ := os.Create("testdb/testofrd10.txt")
	for i := 0; i < 5000; i++ {
		word := genword() //genwordlarge()
		word2 := genword()
		bigwordtestf.Write([]byte(string(word) + "\t" + string(word2)))
		bigwordtestf.Write([]byte("\n"))
		//fmt.Println("word1:", word, string(word))
		//fmt.Println("word2:", word2, string(word2))
		testof.PushKey(word2, word)
		allword2[string(word2)] = word

		for word3, word2 := range allword2 {
			fullword, bfill := testof.FillKey([]byte(word3))
			if bfill == false || string(fullword) != word3+"\t"+string(word2) {
				fmt.Println("error", len(allword2), bfill, "right:", word3+"\t"+string(word2), "filled:", string(fullword))
				time.Sleep(5 * time.Second)
				fullword, bfill := testof.FillKey([]byte(word3))
				fmt.Println(bfill, string(fullword))
				panic("error")
			}
		}
	}
	bigwordtestf.Close()
	testof.Close()
	OrderFileClear(orderpath)
}

func TestOrderFile11(t *testing.T) {
	orderpath := "testdb/TestOrderFile11"
	OrderFileClear(orderpath)
	testof, _ := OpenOrderFile(orderpath, 0, 0, nil)
	testof.RealPush([]byte("abcogkgk"))
	testof.RealPush([]byte("abckdkfkd"))
	testof.RealPush([]byte("dsgfadgfd"))
	testof.RealPush([]byte("zfgghfh"))
	testof.RealPush([]byte("dsgcfllfg"))

	fullword, bfill := testof.RealFill([]byte("abcog"))
	if bfill == false || string(fullword) != "abcogkgk" {
		fmt.Println("error")
		panic("error")
	}

	fullword, bfill = testof.RealFill([]byte("zfgg"))
	if bfill == false || string(fullword) != "zfgghfh" {
		fmt.Println("error")
		panic("error")
	}

	fullword, bfill = testof.RealFill([]byte("dsgc"))
	if bfill == false || string(fullword) != "dsgcfllfg" {
		fmt.Println("error")
		panic("error")
	}
	testof.Close()
	OrderFileClear(orderpath)
}
