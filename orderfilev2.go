//epowsal software iwlb@outlook.com all version all right reserved.
//github.com/epowsal/orderfile32
package orderfile32

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"filelock"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epowsal/orderfile32/orderfiletool"
	"github.com/epowsal/orderfile32/sysidlemem"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

var serialfmap sync.Map

type OrderFile struct {
	path              string
	shorttable        []byte
	cellsize          uint32
	pathfile          *os.File
	datablockcnt      uint32
	isopen            bool
	ordermu           *sync.RWMutex
	lastblockendpos   uint64
	lastblockorderend uint64
	cachemap          sync.Map
	isquit            bool
	isheadmodified    bool
	isflushnow        bool
	lastblockind      uint32

	autoflushinterval        int64
	flushnowch               chan uint8
	istimeoutsync            bool
	releasespacels           map[uint32][]byte
	fileendwritebuf          []byte
	fileendwritebufsize      uint64
	fileendwritebuffilepos   uint64
	fileendwritebufcurlen    uint64
	benablewritelastloadfile bool
	bstatblockspace          bool
	brecordzeroboat          bool
	CompressAndSavewg        sync.WaitGroup
	totalsize                int64
	totalsizelasttime        int64
	extranbuf                []byte
	totalcount               int64
	bhavenewwrite            bool
	sysminidlemem            int64
	presynctime              int64
	idlememflushinterval     int64
	totalcachecount          int64
	toatalreusecount         int64
	maxreusecount            int64
	//markrmmap                sync.Map
	//markpushmap              sync.Map
	//markrmpushmap            sync.Map
	markmu                *sync.RWMutex
	markrmpushfile        *os.File
	prepareflush          bool
	prepareflushendch     chan uint8
	autoflush             bool
	readfullboatmu        sync.Mutex
	spacelen_pos          map[uint32][]byte
	pathlockid            string
	enablermpushlog       bool
	compresstype          int
	lastcompleteorderset  []uint32
	fakelastblockendpos   uint64
	splitcnt              int
	splittype             int
	fixkeylen             int
	fixkeyendbt           []byte
	markrmpushfilemaxsize int64
	flockid               int64
	version               int32
}

const (
	CompressType_Zip = iota
	CompressType_Snappy
	CompressType_Lz4
	CompressType_Xz
	CompressType_Flate
)

//db 6th byte is the compress type. defautl flush interval 2 minute.
func OpenOrderFile(path string, compresstype int, fixkeylength int, fixkeyendbyte []byte) (*OrderFile, error) {
	path = orderfiletool.StdPath(path)
	orderf := &OrderFile{path: path, cellsize: 4096, compresstype: compresstype}
	oerr := orderf.open(path, compresstype, fixkeylength, fixkeyendbyte)
	if oerr != nil {
		return nil, oerr
	}
	return orderf, nil
}

var allowmultiopenfortest bool = false //for test crash recover
func SetDebug() {
	allowmultiopenfortest = true
}

var forbidReopenMap sync.Map

//default open with 5GB log file max size.kv max db size is 32GB.max value size about 128KB.
//if open exists orderfile. the option compresstype and fixkeylength and fixkeyendbyte will ignore.
func (orderf *OrderFile) open(path string, compresstype int, fixkeylength int, fixkeyendbyte []byte) error {
	//orderf.Close()

	orderf.compresstype = compresstype
	orderf.fixkeylen = fixkeylength
	orderf.fixkeyendbt = fixkeyendbyte
	orderf.version = 48

	abspath := orderfiletool.StdUnixLikePath(orderfiletool.ToAbsolutePath(orderf.path))
	// testlogf, _ := os.OpenFile("testlog"+strconv.FormatInt(time.Now().Unix(), 10)+"-"+strconv.FormatInt(rand.Int63(), 10)+".txt", os.O_CREATE|os.O_WRONLY, 0666)
	// testlogf.Write([]byte(abspath + "\n"))
	// buf := make([]byte, 1<<21)
	// bufn := runtime.Stack(buf, true)
	// if bufn >= 0 {
	// 	buf = buf[:bufn]
	// 	testlogf.Write(buf)
	// }
	// buf = []byte{}
	// testlogf.Close()
	orderf.flockid = filelock.Lock(abspath)
	if orderf.flockid <= 0 {
		return errors.New("lock database error")
	}
	_, beopened := forbidReopenMap.LoadOrStore(abspath, 1)
	if allowmultiopenfortest == false && beopened {
		//panic(abspath + " have been open by other process.")
		return errors.New(abspath + " was opened by other process.")
	}
	orderf.pathlockid = abspath
	if allowmultiopenfortest {
		forbidReopenMap.Delete(orderf.pathlockid)
		orderf.pathlockid = ""
		filelock.Unlock(orderf.flockid)
	}
	_, rmpushe := os.Stat(orderf.path + ".rmpush")
	_, rmpushinsavee := os.Stat(orderf.path + ".rmpushinsave")
	_, heade := os.Stat(orderf.path + ".head")
	_, headsavee := os.Stat(orderf.path + ".headsave")
	_, bfreee := os.Stat(orderf.path + ".bfree")
	_, bfreesavee := os.Stat(orderf.path + ".bfreesave")
	_, ffreee := os.Stat(orderf.path + ".ffree")
	_, ffreesavee := os.Stat(orderf.path + ".ffreesave")
	if rmpushe != nil && rmpushinsavee != nil || heade != nil && headsavee != nil || bfreee != nil && bfreesavee != nil || ffreee != nil && ffreesavee != nil {
		OrderFileClear(path)
	}
	_, opterr := os.Stat(path + ".opt")
	if opterr == nil {
		ctt, ctte := orderfiletool.ReadFile(path + ".opt")
		if ctte != nil {
			forbidReopenMap.Delete(orderf.pathlockid)
			filelock.Unlock(orderf.flockid)
			return errors.New("read option file error")
		}
		orderf.compresstype = int(binary.BigEndian.Uint32(ctt[4:8]))
		orderf.fixkeylen = int(binary.BigEndian.Uint32(ctt[8:12]))
		dbversion := int32(binary.BigEndian.Uint32(ctt[12:16]))
		if dbversion != orderf.version {
			forbidReopenMap.Delete(orderf.pathlockid)
			filelock.Unlock(orderf.flockid)
			return errors.New("database version not support. this database version is " + strconv.Itoa(int(dbversion)) + ". support version is " + strconv.Itoa(int(orderf.version)) + ". ")
		}
		endbtlen := int(binary.BigEndian.Uint32(ctt[16:20]))
		orderf.fixkeyendbt = BytesClone(ctt[20 : 20+endbtlen])

	} else {
		bt := make([]byte, 16)
		binary.BigEndian.PutUint32(bt[4:8], uint32(orderf.compresstype))
		binary.BigEndian.PutUint32(bt[8:12], uint32(orderf.fixkeylen))
		binary.BigEndian.PutUint32(bt[12:16], uint32(orderf.version))
		tempbt := make([]byte, 4)
		binary.BigEndian.PutUint32(tempbt, uint32(len(orderf.fixkeyendbt)))
		bt = append(bt, tempbt...)
		bt = append(bt, orderf.fixkeyendbt...)
		orderfiletool.WriteFile(path+".opt", bt)
	}

	_, corfstateerror := os.Stat(path + ".beopen")
	if corfstateerror == nil {
		fmt.Println(path + ":maybe did not correct close. remove the .beopen file can to recover it.")
		//panic(path + ":maybe did not correct close. remove the .beopen file can to recover it.")
		if allowmultiopenfortest {
			Backup(path)
		}
	}
	orderf.path = path

	if strings.Index(path, "/") != -1 {
		dirpath := path[:strings.LastIndex(path, "/")]
		os.MkdirAll(dirpath, 0666)
	}
	rand.Seed(time.Now().UnixNano())
	orderf.releasespacels = make(map[uint32][]byte, 0)
	orderf.spacelen_pos = make(map[uint32][]byte, 0)
	orderf.extranbuf = make([]byte, 5*(1<<17))
	var pathfile *os.File
	var err, fexistserr error
	fi, fexistserr := os.Stat(path)
	if fexistserr == nil && fi.IsDir() {
		forbidReopenMap.Delete(orderf.pathlockid)
		filelock.Unlock(orderf.flockid)
		return errors.New("database is directory already exists. open database error.")
	}

	if fexistserr != nil {
		pathfile, err = os.Create(path)
		if err != nil {
			forbidReopenMap.Delete(orderf.pathlockid)
			filelock.Unlock(orderf.flockid)
			return errors.New("database file create error.")
		}
	} else {
		pathfile, err = os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			forbidReopenMap.Delete(orderf.pathlockid)
			filelock.Unlock(orderf.flockid)
			return errors.New("database file open error.")
		}
	}
	if err == nil {
		orderf.pathfile = pathfile
		endpos, _ := pathfile.Seek(0, os.SEEK_END)
		if endpos == 0 || !(heade == nil || headsavee == nil) {
			orderf.setNewFileData()
			orderf.cachemap.Store(uint32(0), orderf.setValTouch([]byte{0}, 0, false))
			orderf.totalcachecount = 1
		} else {
			pathfile.Seek(0, os.SEEK_SET)
			firstctt := make([]byte, 5)
			rdn, rde := pathfile.Read(firstctt)
			if rde != nil || rdn == 5 && orderfiletool.CheckZero(firstctt) {
				fmt.Println(orderf.path + " first content zero recover reinitialize orderfile.")
				pathfile.Seek(0, os.SEEK_SET)
				orderf.setNewFileData()
				orderf.cachemap.Store(uint32(0), orderf.setValTouch([]byte{0}, 0, false))
				orderf.totalcachecount = 1
			}
			orderf.totalcachecount = 0
			if heade == nil {
				stdata, _ := orderfiletool.ReadFile(orderf.path + ".head")
				stdataplain, _ := orderfiletool.ReadFile(orderf.path + ".headplain")
				if orderf.compresstype == 0 {
					orderf.shorttable = ZipDecode(nil, stdata)
				} else if orderf.compresstype == 1 {
					orderf.shorttable = SnappyDecode(nil, stdata)
				} else if orderf.compresstype == 2 {
					orderf.shorttable = Lz4Decode(nil, stdata)
				} else if orderf.compresstype == 3 {
					orderf.shorttable = XzDecode(nil, stdata)
				} else if orderf.compresstype == 4 {
					orderf.shorttable = FlateDecode(nil, stdata)
				}
				if len(stdataplain) > 0 && bytes.Compare(stdataplain, orderf.shorttable) != 0 {
					panic("short table uncompress error.")
				}
				if len(orderf.shorttable) == 0 {
					if len(stdata) < 512 && orderfiletool.CheckZero(stdata) {
						fmt.Println(orderf.path + " head zero recover reinitialize orderfile.")
						pathfile.Seek(0, os.SEEK_SET)
						orderf.setNewFileData()
						orderf.cachemap.Store(uint32(0), orderf.setValTouch([]byte{0}, 0, false))
						orderf.totalcachecount = 1
					} else {
						fmt.Println(orderf.path, orderf.datablockcnt, endpos, stdata, orderf.shorttable)
						panic(orderf.path + ":datablockcnt error! maybe initialize be broken.")
					}
				}
				orderf.datablockcnt = binary.BigEndian.Uint32(orderf.shorttable[6:10])
				if orderf.datablockcnt <= 0 {
					fmt.Println(orderf.path, orderf.datablockcnt, endpos, stdata, orderf.shorttable)
					panic(orderf.path + ":datablockcnt error! maybe instance did not killed.")
				}
				orderf.lastblockind = uint32(binary.BigEndian.Uint32(orderf.shorttable[10:14]))

				orderf.totalcount = (int64(binary.BigEndian.Uint16(orderf.shorttable[18:20])) << 32) | int64(binary.BigEndian.Uint32(orderf.shorttable[20:24]))
				orderstart, orderlen, _, _, _ := getBlockPos(orderf.shorttable[24+orderf.lastblockind*11 : 24+orderf.lastblockind*11+2*11])
				orderf.lastblockorderend = orderstart + orderlen
			}
			_, bfreee := os.Stat(orderf.path + ".bfree")
			if bfreee == nil {
				bfreectt, _ := orderfiletool.ReadFile(orderf.path + ".bfree")
				if orderf.compresstype == 0 {
					debfree := ZipDecode(nil, bfreectt)
					orderf.releasespacels = BytesToMapU32Bytes(debfree)
				} else if orderf.compresstype == 1 {
					debfree := SnappyDecode(nil, bfreectt)
					orderf.releasespacels = BytesToMapU32Bytes(debfree)
				} else if orderf.compresstype == 2 {
					debfree := Lz4Decode(nil, bfreectt)
					orderf.releasespacels = BytesToMapU32Bytes(debfree)
				} else if orderf.compresstype == 3 {
					debfree := XzDecode(nil, bfreectt)
					orderf.releasespacels = BytesToMapU32Bytes(debfree)
				} else if orderf.compresstype == 4 {
					debfree := FlateDecode(nil, bfreectt)
					orderf.releasespacels = BytesToMapU32Bytes(debfree)
				}
			}
			_, ffreee := os.Stat(orderf.path + ".ffree")
			if ffreee == nil {
				ffreectt, _ := orderfiletool.ReadFile(orderf.path + ".ffree")
				if orderf.compresstype == 0 {
					deffree := ZipDecode(nil, ffreectt)
					orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
				} else if orderf.compresstype == 1 {
					deffree := SnappyDecode(nil, ffreectt)
					orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
				} else if orderf.compresstype == 2 {
					deffree := Lz4Decode(nil, ffreectt)
					orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
				} else if orderf.compresstype == 3 {
					deffree := XzDecode(nil, ffreectt)
					orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
				} else if orderf.compresstype == 4 {
					deffree := FlateDecode(nil, ffreectt)
					orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
				}
			}
		}
	} else {
		panic(orderf.path + ":open order file failed!")
	}
	orderf.isopen = true
	orderf.isquit = false
	orderf.autoflushinterval = 120
	orderf.ordermu = &sync.RWMutex{}
	orderf.fileendwritebufsize = 1 * 1024 * 1024
	orderf.fileendwritebuf = make([]byte, orderf.fileendwritebufsize+1024)
	orderf.fileendwritebufcurlen = 0
	orderf.brecordzeroboat = true
	orderf.isheadmodified = false
	orderf.flushnowch = make(chan uint8, 0)
	orderf.isflushnow = false
	orderf.istimeoutsync = false
	orderf.totalsize = 0
	orderf.totalsizelasttime = 0
	orderf.presynctime = time.Now().Unix()
	orderf.idlememflushinterval = 30
	orderf.markmu = &sync.RWMutex{}
	orderf.sysminidlemem = 640 * 1024 * 1024
	orderf.toatalreusecount = 0
	orderf.maxreusecount = 0
	orderf.prepareflush = false
	orderf.prepareflushendch = make(chan uint8, 0)
	orderf.autoflush = true
	orderf.enablermpushlog = true
	orderf.lastcompleteorderset = make([]uint32, 0, 1024)
	orderf.markrmpushfilemaxsize = 5 * 1024 * 1024 * 1024

	//find recover data deal
	_, rmpushtemper := os.Stat(orderf.path + ".rmpushtempok")
	if rmpushtemper == nil {
		_, rmpushtempe := os.Stat(orderf.path + ".rmpushtemp")
		if rmpushtempe == nil {
			os.Remove(orderf.path + ".rmpush")
			os.Rename(orderf.path+".rmpushtemp", orderf.path+".rmpush")
		}
		os.Remove(orderf.path + ".rmpushtempok")
	}
	_, err2 := os.Stat(orderf.path + ".rmpush")
	if err2 == nil {
		orderf.markrmpushfile, _ = os.OpenFile(orderf.path+".rmpush", os.O_RDWR, 0666)
	} else {
		time.Sleep(10 * time.Millisecond)
		orderf.markrmpushfile, _ = os.Create(orderf.path + ".rmpush")
	}
	endpos, _ := orderf.markrmpushfile.Seek(0, os.SEEK_END)
	cvtbuf := make([]byte, 8)
	if endpos > 0 {
		orderf.markrmpushfile.Seek(0, os.SEEK_SET)
		readfrom := int64(0)
		curi := int64(0)
		readlen := int64(0)
		loadcount := 0
		bnormalbreak := true
		normalendpos := int64(0)
		for i := int64(0); i < endpos; {
			readlen = int64(len(orderf.fileendwritebuf)) - readfrom
			if i+readlen > endpos {
				readlen = endpos - i
			}
			_, readerr := orderf.markrmpushfile.Read(orderf.fileendwritebuf[readfrom : readfrom+readlen])
			if readerr != nil {
				break
			}
			curi = int64(0)
			for true {
				if curi+4 > readfrom+readlen {
					if curi == readfrom+readlen {
						bnormalbreak = true
					} else {
						bnormalbreak = false
					}
					copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
					readfrom = readfrom + readlen - curi
					break
				}
				val := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi : curi+4]))
				var bdel bool
				if val >= (1 << 31) {
					val -= (1 << 31)
					bdel = true
				}
				if curi+4+val > readfrom+readlen {
					copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
					readfrom = readfrom + readlen - curi
					bnormalbreak = false
					break
				}
				if bdel {
					//orderf.markrmpushmap.Delete(string(orderf.fileendwritebuf[curi+4 : curi+4+val]))
					//orderf.markrmmap.Store(string(orderf.fileendwritebuf[curi+4:curi+4+val]), true)
					orderf.RealRm(BytesCombine(orderf.fileendwritebuf[curi+4:curi+4+val], orderf.fixkeyendbt))
					curi += 4 + val
					normalendpos = i + curi
				} else {
					key := orderf.fileendwritebuf[curi+4 : curi+4+val]
					if curi+4+val+4 > readfrom+readlen {
						copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
						readfrom = readfrom + readlen - curi
						bnormalbreak = false
						break
					}
					val2 := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi+4+val : curi+4+val+4]))
					if curi+4+val+4+val2 > readfrom+readlen {
						copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
						readfrom = readfrom + readlen - curi
						bnormalbreak = false
						break
					}
					//orderf.markrmmap.Delete(string(key))
					//orderf.markrmpushmap.Store(string(key), BytesClone(orderf.fileendwritebuf[curi+4+val+4:curi+4+val+4+val2]))
					orderf.RealRmPush(BytesCombine(key, orderf.fixkeyendbt), orderf.fileendwritebuf[curi+4+val+4:curi+4+val+4+val2])
					loadcount += 1
					curi += 4 + val + 4 + val2
					normalendpos = i + curi
				}
			}
			i += readlen
		}
		if bnormalbreak == false {
			// orderf.markrmpushfile.Close()
			// os.Rename(orderf.path+".rmpush", orderf.path+".broken"+strconv.FormatInt(time.Now().Unix(), 10)+".rmpush")
			// orderf.markrmpushfile, _ = os.Create(orderf.path + ".rmpush")
			// orderf.markrmmap.Range(func(key, val interface{}) bool {
			// 	keylen := uint32(len([]byte(key.(string)))) | (uint32(1) << 31)
			// 	binary.BigEndian.PutUint32(cvtbuf, keylen)
			// 	orderf.markrmpushfile.Write(cvtbuf[:4])
			// 	orderf.markrmpushfile.Write([]byte(key.(string)))
			// 	return true
			// })
			// orderf.markrmpushmap.Range(func(key, val interface{}) bool {
			// 	keylen := uint32(len([]byte(key.(string))))
			// 	binary.BigEndian.PutUint32(cvtbuf, keylen)
			// 	orderf.markrmpushfile.Write(cvtbuf[:4])
			// 	orderf.markrmpushfile.Write([]byte(key.(string)))

			// 	vallen := uint32(len(val.([]byte)))
			// 	binary.BigEndian.PutUint32(cvtbuf, vallen)
			// 	orderf.markrmpushfile.Write(cvtbuf[:4])
			// 	orderf.markrmpushfile.Write(val.([]byte))
			// 	return true
			// })
			// orderf.markrmpushfile.Sync()

			orderf.markrmpushfile.Truncate(normalendpos)
		}
		fmt.Println(orderf.path, "Load Count:", loadcount)
	}

	_, savendstat := os.Stat(orderf.path + ".headsaveok")

	if savendstat == nil {
		_, newsavedataexisterr := os.Stat(orderf.path + ".newsavedata")
		if newsavedataexisterr == nil {
			oldrecoverfile, oldrecoverfilee := os.OpenFile(orderf.path+".newsavedata", os.O_RDWR, 0666)
			if oldrecoverfilee == nil {
				endpos, _ := oldrecoverfile.Seek(0, os.SEEK_END)
				if endpos > 0 {
					oldrecoverfile.Seek(0, os.SEEK_SET)
					readfrom := int64(0)
					i := int64(0)
					for i < endpos {
						readlen := int64(len(orderf.fileendwritebuf)) - readfrom
						if i+readlen > endpos {
							readlen = endpos - i
						}
						_, readerr := oldrecoverfile.Read(orderf.fileendwritebuf[readfrom : readfrom+readlen])
						if readerr != nil {
							break
						}
						i += readlen
						curi := int64(0)
						for true {
							if curi+12 > readfrom+readlen {
								copy(orderf.fileendwritebuf, orderf.fileendwritebuf[curi:readfrom+readlen])
								readfrom = readfrom + readlen - curi
								break
							}
							writepos := int64(binary.BigEndian.Uint64(orderf.fileendwritebuf[curi : curi+8]))
							datalen := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi+8 : curi+8+4]))
							if curi+8+4+datalen > readfrom+readlen {
								copy(orderf.fileendwritebuf, orderf.fileendwritebuf[curi:readfrom+readlen])
								readfrom = readfrom + readlen - curi
								break
							}
							orderf.pathfile.Seek(writepos, os.SEEK_SET)
							orderf.pathfile.Write(orderf.fileendwritebuf[curi+8+4 : curi+8+4+datalen])
							curi += 8 + 4 + datalen
						}
					}
					if i != endpos {
						panic(orderf.path + " file crash error.")
					}
				}
				oldrecoverfile.Close()
			}
		}

		endpos2, endpos2e := orderf.pathfile.Seek(0, os.SEEK_END)
		if endpos2e != nil {
			panic("seek error 435")
		}
		dataflenbt := make([]byte, 8)
		binary.BigEndian.PutUint64(dataflenbt, uint64(endpos2))
		_, er := orderf.pathfile.Seek(0, os.SEEK_SET)
		if er == nil {
			orderf.pathfile.Write(dataflenbt[3:])
		} else {
			panic("seek error 425")
		}
		orderf.pathfile.Sync()

		_, heade := os.Stat(orderf.path + ".headsave")
		if heade == nil {
			_, heade := os.Stat(orderf.path + ".head")
			if heade == nil {
				os.Remove(orderf.path + ".head.backup")
			}
			os.Rename(orderf.path+".head", orderf.path+".head.backup")
			os.Rename(orderf.path+".headsave", orderf.path+".head")

			stdata, _ := orderfiletool.ReadFile(orderf.path + ".head")
			if orderf.compresstype == 0 {
				orderf.shorttable = ZipDecode(nil, stdata)
			} else if orderf.compresstype == 1 {
				orderf.shorttable = SnappyDecode(nil, stdata)
			} else if orderf.compresstype == 2 {
				orderf.shorttable = Lz4Decode(nil, stdata)
			} else if orderf.compresstype == 3 {
				orderf.shorttable = XzDecode(nil, stdata)
			} else if orderf.compresstype == 4 {
				orderf.shorttable = FlateDecode(nil, stdata)
			}
			orderf.datablockcnt = binary.BigEndian.Uint32(orderf.shorttable[6:10])
			if orderf.datablockcnt <= 0 {
				fmt.Println(orderf.path, orderf.datablockcnt)
				panic(orderf.path + ":datablockcnt error 326.")
			}
			orderf.lastblockind = uint32(binary.BigEndian.Uint32(orderf.shorttable[10:14]))

			orderf.totalcount = (int64(binary.BigEndian.Uint16(orderf.shorttable[18:20])) << 32) | int64(binary.BigEndian.Uint32(orderf.shorttable[20:24]))
			orderstart, orderlen, _, _, _ := getBlockPos(orderf.shorttable[24+orderf.lastblockind*11 : 24+orderf.lastblockind*11+2*11])
			orderf.lastblockorderend = orderstart + orderlen
		}

		_, bfreee := os.Stat(orderf.path + ".bfreesave")
		if bfreee == nil {
			_, heade := os.Stat(orderf.path + ".bfree")
			if heade == nil {
				os.Remove(orderf.path + ".bfree.backup")
			}
			os.Rename(orderf.path+".bfree", orderf.path+".bfree.backup")
			os.Rename(orderf.path+".bfreesave", orderf.path+".bfree")

			bfreectt, _ := orderfiletool.ReadFile(orderf.path + ".bfree")
			if orderf.compresstype == 0 {
				debfree := ZipDecode(nil, bfreectt)
				orderf.releasespacels = BytesToMapU32Bytes(debfree)
			} else if orderf.compresstype == 1 {
				debfree := SnappyDecode(nil, bfreectt)
				orderf.releasespacels = BytesToMapU32Bytes(debfree)
			} else if orderf.compresstype == 2 {
				debfree := Lz4Decode(nil, bfreectt)
				orderf.releasespacels = BytesToMapU32Bytes(debfree)
			} else if orderf.compresstype == 3 {
				debfree := XzDecode(nil, bfreectt)
				orderf.releasespacels = BytesToMapU32Bytes(debfree)
			} else if orderf.compresstype == 4 {
				debfree := FlateDecode(nil, bfreectt)
				orderf.releasespacels = BytesToMapU32Bytes(debfree)
			}
		}

		_, ffreee := os.Stat(orderf.path + ".ffreesave")
		if ffreee == nil {
			_, heade := os.Stat(orderf.path + ".ffree")
			if heade == nil {
				os.Remove(orderf.path + ".ffree.backup")
			}
			os.Rename(orderf.path+".ffree", orderf.path+".ffree.backup")
			os.Rename(orderf.path+".ffreesave", orderf.path+".ffree")

			ffreectt, _ := orderfiletool.ReadFile(orderf.path + ".ffree")
			if orderf.compresstype == 0 {
				orderf.spacelen_pos = BytesToMapU32Bytes(ZipDecode(nil, ffreectt))
			} else if orderf.compresstype == 1 {
				deffree := SnappyDecode(nil, ffreectt)
				orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
			} else if orderf.compresstype == 2 {
				deffree := Lz4Decode(nil, ffreectt)
				orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
			} else if orderf.compresstype == 3 {
				deffree := XzDecode(nil, ffreectt)
				orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
			} else if orderf.compresstype == 4 {
				deffree := FlateDecode(nil, ffreectt)
				orderf.spacelen_pos = BytesToMapU32Bytes(deffree)
			}

		}

		//exec.Command("sync")
		os.Remove(orderf.path + ".rmpushinmem")
		os.Remove(orderf.path + ".rmpushinsave")
		os.Remove(orderf.path + ".newsavedata")
		os.Remove(orderf.path + ".headsaveok")
	} else {
		_, insavestat := os.Stat(orderf.path + ".rmpushinsave")
		if insavestat == nil {
			saveingf, _ := os.OpenFile(orderf.path+".rmpushinsave", os.O_RDWR, 0666)
			if saveingf != nil {
				endpos, _ := saveingf.Seek(0, os.SEEK_END)
				if endpos > 0 {
					saveingf.Seek(0, os.SEEK_SET)
					readfrom := int64(0)
					curi := int64(0)
					//markrmmap := &sync.Map{}
					//markrmpushmap := &sync.Map{}
					readlen := int64(0)
					bnormalbreak := true
					normalendpos := int64(0)
					rmpushrempf, _ := os.Create(orderf.path + ".rmpushtemp")
					for i := int64(0); i < endpos; {
						readlen = int64(len(orderf.fileendwritebuf)) - readfrom
						if i+readlen > endpos {
							readlen = endpos - i
						}
						_, readerr := saveingf.Read(orderf.fileendwritebuf[readfrom : readfrom+readlen])
						if readerr != nil {
							break
						}
						curi = int64(0)
						for true {
							if curi+4 > readfrom+readlen {
								if curi == readfrom+readlen {
									bnormalbreak = true
								} else {
									bnormalbreak = false
								}
								copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
								readfrom = readfrom + readlen - curi
								break
							}
							val := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi : curi+4]))
							var bdel bool
							if val >= (1 << 31) {
								val -= (1 << 31)
								bdel = true
							}
							if curi+4+val > readfrom+readlen {
								copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
								readfrom = readfrom + readlen - curi
								bnormalbreak = false
								break
							}
							if bdel {
								// _, ok1 := orderf.markrmmap.Load(string(orderf.fileendwritebuf[curi+4 : curi+4+val]))
								// _, ok2 := orderf.markrmpushmap.Load(string(orderf.fileendwritebuf[curi+4 : curi+4+val]))
								// if ok1 == false && ok2 == false {
								// 	markrmpushmap.Delete(string(orderf.fileendwritebuf[curi+4 : curi+4+val]))
								// 	markrmmap.Store(string(orderf.fileendwritebuf[curi+4:curi+4+val]), true)
								// }

								keylen := uint32(uint32(len(orderf.fileendwritebuf[curi+4:curi+4+val])) | (uint32(1) << 31))
								binary.BigEndian.PutUint32(cvtbuf, keylen)
								rmpushrempf.Write(cvtbuf[:4])
								rmpushrempf.Write(orderf.fileendwritebuf[curi+4 : curi+4+val])
								orderf.RealRm(BytesCombine(orderf.fileendwritebuf[curi+4:curi+4+val], orderf.fixkeyendbt))
								curi += 4 + val
								normalendpos = i + curi
							} else {
								key := orderf.fileendwritebuf[curi+4 : curi+4+val]
								if curi+4+val+4 > readfrom+readlen {
									copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
									readfrom = readfrom + readlen - curi
									bnormalbreak = false
									break
								}
								val2 := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi+4+val : curi+4+val+4]))
								if curi+4+val+4+val2 > readfrom+readlen {
									copy(orderf.fileendwritebuf[0:], orderf.fileendwritebuf[curi:readfrom+readlen])
									readfrom = readfrom + readlen - curi
									bnormalbreak = false
									break
								}
								// _, ok1 := orderf.markrmmap.Load(string(orderf.fileendwritebuf[curi+4+val+4 : curi+4+val+4+val2]))
								// _, ok2 := orderf.markrmpushmap.Load(string(orderf.fileendwritebuf[curi+4+val+4 : curi+4+val+4+val2]))
								// if ok1 == false && ok2 == false {
								// 	markrmmap.Delete(string(key))
								// 	markrmpushmap.Store(string(key), BytesClone(orderf.fileendwritebuf[curi+4+val+4:curi+4+val+4+val2]))
								// }

								keylen := uint32(len(key))
								binary.BigEndian.PutUint32(cvtbuf, keylen)
								rmpushrempf.Write(cvtbuf[:4])
								rmpushrempf.Write(key)

								vallen := uint32(len(orderf.fileendwritebuf[curi+4+val+4 : curi+4+val+4+val2]))
								binary.BigEndian.PutUint32(cvtbuf, vallen)
								rmpushrempf.Write(cvtbuf[:4])
								rmpushrempf.Write(orderf.fileendwritebuf[curi+4+val+4 : curi+4+val+4+val2])

								orderf.RealRmPush(BytesCombine(key, orderf.fixkeyendbt), orderf.fileendwritebuf[curi+4+val+4:curi+4+val+4+val2])
								curi += 4 + val + 4 + val2
								normalendpos = i + curi
							}
						}
						i += readlen
					}
					// if bnormalbreak == false {
					// 	//maybe need not merge
					// }
					// markrmmap.Range(func(key, val interface{}) bool {
					// 	orderf.markrmmap.Store(key, true)
					// 	return true
					// })
					// markrmpushmap.Range(func(key, val interface{}) bool {
					// 	orderf.markrmpushmap.Store(key, val)
					// 	return true
					// })
					// //build new log
					// rmpushrempf, _ := os.Create(orderf.path + ".rmpushtemp")
					// orderf.markrmmap.Range(func(key, val interface{}) bool {
					// 	keylen := uint32(len([]byte(key.(string)))) | (uint32(1) << 31)
					// 	binary.BigEndian.PutUint32(cvtbuf, keylen)
					// 	rmpushrempf.Write(cvtbuf[:4])
					// 	rmpushrempf.Write([]byte(key.(string)))
					// 	return true
					// })
					// orderf.markrmpushmap.Range(func(key, val interface{}) bool {
					// 	keylen := uint32(len([]byte(key.(string))))
					// 	binary.BigEndian.PutUint32(cvtbuf, keylen)
					// 	rmpushrempf.Write(cvtbuf[:4])
					// 	rmpushrempf.Write([]byte(key.(string)))

					// 	vallen := uint32(len(val.([]byte)))
					// 	binary.BigEndian.PutUint32(cvtbuf, vallen)
					// 	rmpushrempf.Write(cvtbuf[:4])
					// 	rmpushrempf.Write(val.([]byte))
					// 	return true
					// })
					// rmpushrempf.Close()
					// orderf.markrmpushfile.Sync()
					// //exec.Command("sync")
					// //time.Sleep(10 * time.Millisecond)
					// orderfiletool.WriteFile(orderf.path+".rmpushtempok", []byte{})
					// orderf.markrmpushfile.Close()

					rmpushrempf.Sync()
					rmpushrempf.Close()

					os.Remove(orderf.path + ".rmpush")
					os.Rename(orderf.path+".rmpushtemp", orderf.path+".rmpush")
					//exec.Command("sync")
					orderf.markrmpushfile, _ = os.OpenFile(orderf.path+".rmpush", os.O_RDWR, 0666)
					orderf.markrmpushfile.Seek(0, os.SEEK_END)

					if bnormalbreak == false {
						saveingf.Seek(normalendpos, os.SEEK_SET)
						erdata := make([]byte, endpos-normalendpos)
						saveingf.Read(erdata)
						orderfiletool.WriteFile(orderf.path+".rmpushinsave.bad", erdata)
					}
				}
				saveingf.Close()
			}
			os.Remove(orderf.path + ".rmpushinmem")
			os.Remove(orderf.path + ".headsave")
			os.Remove(orderf.path + ".bfreesave")
			os.Remove(orderf.path + ".ffreesave")
			os.Remove(orderf.path + ".newsavedata")
			os.Remove(orderf.path + ".rmpushinsave")
			os.Remove(orderf.path + ".rmpushtempok")
		}
	}
	_, er := orderf.pathfile.Seek(0, os.SEEK_SET)
	if er != nil {
		panic("seek error 657")
	}
	endposbt := make([]byte, 9)
	orderf.pathfile.Read(endposbt[3:])
	if int(endposbt[8]) != orderf.compresstype {
		forbidReopenMap.Delete(orderf.pathlockid)
		filelock.Unlock(orderf.flockid)
		return errors.New("database compress type error.")
	}
	orderf.lastblockendpos = uint64(binary.BigEndian.Uint64(endposbt))
	orderf.pathfile.Truncate(int64(orderf.lastblockendpos))
	orderf.fileendwritebuffilepos = orderf.lastblockendpos
	orderf.fakelastblockendpos = orderf.lastblockendpos
	if orderf.fakelastblockendpos == 0 {
		panic(orderf.path + " fakelastblockendpos zero error")
	}

	orderfiletool.WriteFile(path+".beopen", []byte{})
	//orderf.ErrorRecord("O", []byte(orderf.path), nil)

	orderf.fileendwritebuf = []byte{}
	go BlockFlush(orderf)
	return nil
}
func (orderf *OrderFile) printSortTable() {
	for i := uint32(0); i < orderf.datablockcnt; i++ {
		os, ol, dp, dl, _ := getBlockPos(orderf.shorttable[24+i*11 : 24+i*11+2*11])
		fmt.Println(i, os, ol, dp, dl, orderf.shorttable[24+i*11:24+i*11+2*11])
	}
}

func (orderf *OrderFile) ErrorRecord(str string, data []byte, data2 []byte) {
	// if strings.Index(orderf.path, "/kkkkdfdsf/") != -1 {
	// 	return
	// }
	// var serialf *os.File
	// serialf2, blodk := serialfmap.Load(orderf.path + "_err.log")
	// if !blodk {
	// 	_, errr := os.Stat(orderf.path + "_err.log")
	// 	if errr == nil {
	// 		serialf, _ = os.OpenFile(orderf.path+"_err.log", os.O_RDWR, 0666)
	// 		serialf.Seek(0, os.SEEK_END)
	// 	} else {
	// 		serialf, _ = os.Create(orderf.path + "_err.log")
	// 	}
	// 	serialfmap.Store(orderf.path+"_err.log", serialf)
	// } else {
	// 	serialf = serialf2.(*os.File)
	// }
	// serialf.Write([]byte(str))
	//} else {
	var serialf *os.File
	serialf2, blodk := serialfmap.Load(orderf.path + "_err.log")
	if !blodk {
		_, errr := os.Stat(orderf.path + "_err.log")
		if errr == nil {
			serialf, _ = os.OpenFile(orderf.path+"_err.log", os.O_RDWR, 0666)
			serialf.Seek(0, os.SEEK_END)
		} else {
			serialf, _ = os.Create(orderf.path + "_err.log")
		}
		serialfmap.Store(orderf.path+"_err.log", serialf)
	} else {
		serialf = serialf2.(*os.File)
	}
	//serialf.Write([]byte(str + " " + string(data) + "\n"))

	serialf.Write([]byte(str))
	datalenbt := make([]byte, 4)
	binary.BigEndian.PutUint32(datalenbt, uint32(len(data)))
	serialf.Write(datalenbt)
	serialf.Write(data)
	if data2 != nil {
		datalenbt := make([]byte, 4)
		binary.BigEndian.PutUint32(datalenbt, uint32(len(data2)))
		serialf.Write(datalenbt)
		serialf.Write(data2)
	}
}

func (orderf *OrderFile) setValTouch(data []byte, oldval uint64, bmod bool) []byte {
	if len(data) == 0 {
		panic("data empty error")
	}
	var databt []byte
	oldval += 1
	atomic.AddInt64(&orderf.toatalreusecount, 1)
	if int64(oldval) > orderf.maxreusecount {
		atomic.StoreInt64(&orderf.maxreusecount, int64(oldval))
	}
	endbt := make([]byte, 8)
	if bmod {
		binary.BigEndian.PutUint64(endbt, (oldval<<1)|1)
	} else {
		binary.BigEndian.PutUint64(endbt, (oldval<<1)|0)
	}
	databt = append(data, endbt...)
	return databt
}

func getOldCount(data []byte) uint64 {
	if len(data) < 8 {
		return 1
	}
	return binary.BigEndian.Uint64(data[len(data)-8:]) >> 1
}

func (orderf *OrderFile) setNewFileData() {
	orderf.pathfile.Seek(0, os.SEEK_SET)
	orderf.pathfile.Truncate(0)
	datarealsizebt := make([]byte, 8)
	orderf.pathfile.Write(datarealsizebt[3:])
	orderf.pathfile.Write([]byte{byte(orderf.compresstype)})
	orderf.shorttable = make([]byte, orderf.cellsize)
	orderf.shorttable[0] = 0
	orderf.datablockcnt = 1
	orderf.lastblockind = 0

	orderf.totalcount = 0
	var lastblockbt []byte
	if orderf.compresstype == 0 {
		lastblockbt = ZipEncode(orderf.extranbuf, []byte{0}, 1)
	} else if orderf.compresstype == 1 {
		lastblockbt = SnappyEncode(orderf.extranbuf, []byte{0})
	} else if orderf.compresstype == 2 {
		lastblockbt = Lz4Encode(orderf.extranbuf, []byte{0})
	} else if orderf.compresstype == 3 {
		lastblockbt = XzEncode(orderf.extranbuf, []byte{0})
	} else if orderf.compresstype == 4 {
		lastblockbt = FlateEncode(orderf.extranbuf, []byte{0}, 1)
	}
	binary.BigEndian.PutUint32(orderf.shorttable[6:10], uint32(orderf.datablockcnt))
	binary.BigEndian.PutUint32(orderf.shorttable[10:14], orderf.lastblockind)
	binary.BigEndian.PutUint32(orderf.shorttable[14:18], 0)
	binary.BigEndian.PutUint16(orderf.shorttable[18:20], uint16(orderf.totalcount>>32))
	binary.BigEndian.PutUint32(orderf.shorttable[20:24], uint32(orderf.totalcount&0xFFFFFFFF))
	setBlockPos(orderf.shorttable[24:24+2*11], 0, 1, 6, uint64(len(lastblockbt)), 1)
	orderf.pathfile.Write(lastblockbt)
	binary.BigEndian.PutUint64(datarealsizebt, uint64(6+len(lastblockbt)))
	orderf.pathfile.Seek(0, os.SEEK_SET)
	orderf.pathfile.Write(datarealsizebt[3:])
	orderf.pathfile.Sync()
	orderf.lastblockorderend = 1
	var zst []byte
	if orderf.compresstype == 0 {
		zst = ZipEncode(nil, orderf.shorttable, 1)
	} else if orderf.compresstype == 1 {
		zst = SnappyEncode(nil, orderf.shorttable)
	} else if orderf.compresstype == 2 {
		zst = Lz4Encode(nil, orderf.shorttable)
	} else if orderf.compresstype == 3 {
		zst = XzEncode(nil, orderf.shorttable)
	} else if orderf.compresstype == 4 {
		zst = FlateEncode(nil, orderf.shorttable, 1)
	}
	orderfiletool.WriteFile(orderf.path+".head", zst)
	orderfiletool.WriteFile(orderf.path+".bfree", []byte{})
	orderfiletool.WriteFile(orderf.path+".ffree", []byte{})
}

//false effect is maybe lost data when crash.
func (orderf *OrderFile) EnableRmpushLog(enablermpushlog bool) {
	orderf.enablermpushlog = enablermpushlog
}
func (orderf *OrderFile) setFreeSpaceFromFile(bfreefromfile bool) {
	orderf.benablewritelastloadfile = bfreefromfile
}

func (orderf *OrderFile) setStatBlockSpace(bstat bool) {
	orderf.bstatblockspace = bstat
}

func (orderf *OrderFile) SetRecordZeroBoat(brecord bool) {
	orderf.brecordzeroboat = brecord
}

func (orderf *OrderFile) SetSysMinIdleMem(minidlemem int64) {
	orderf.sysminidlemem = minidlemem
}
func (orderf *OrderFile) SetFlushInterval(flushinterval int64) {
	orderf.autoflushinterval = flushinterval
}

func (orderf *OrderFile) SetLogFileMaxSize(maxsize int64) {
	orderf.markrmpushfilemaxsize = maxsize
}

func MapSerializeV2(mdata sync.Map) []byte {
	var mdatastr []byte
	keybt := make([]byte, 4)
	vallenbt := make([]byte, 4)
	var keyint, vallen uint32
	mdata.Range(func(key, value interface{}) bool {
		keyint = uint32(key.(int))
		binary.BigEndian.PutUint32(keybt, keyint)
		vallen = uint32(len(value.(string)))
		binary.BigEndian.PutUint32(vallenbt, vallen)
		mdatastr = BytesCombine(mdatastr, keybt, vallenbt, []byte(value.(string)))
		return true
	})
	return mdatastr
}

func MapUnserializeV2(mdatastr []byte) sync.Map {
	var mdata sync.Map
	var key, vallen, startpos uint32
	for uint32(startpos)+8 <= uint32(len(mdatastr)) {
		key = binary.BigEndian.Uint32(mdatastr[startpos : startpos+4])
		vallen = binary.BigEndian.Uint32(mdatastr[startpos+4 : startpos+4+4])
		mdata.Store(int(key), string(mdatastr[startpos+4+4:startpos+4+4+vallen]))
		startpos += 4 + 4 + vallen
		if startpos >= uint32(len(mdatastr)) {
			break
		}
	}
	return mdata
}

func MapIntBytesToBytes(mdata sync.Map) (outbt []byte) {
	keybt := make([]byte, 4)
	vallenbt := make([]byte, 4)
	var keyint, vallen uint32
	mdata.Range(func(key, value interface{}) bool {
		keyint = key.(uint32)
		binary.BigEndian.PutUint32(keybt, keyint)
		vallen = uint32(len(value.([]byte)))
		binary.BigEndian.PutUint32(vallenbt, vallen)
		outbt = append(outbt, keybt...)
		outbt = append(outbt, vallenbt...)
		outbt = append(outbt, value.([]byte)...)
		return true
	})
	return outbt
}

func MapU32BytesToBytes(mdata map[uint32][]byte) (outbt []byte) {
	keybt := make([]byte, 4)
	vallenbt := make([]byte, 4)
	var keyint, vallen uint32
	for key, value := range mdata {
		keyint = key
		binary.BigEndian.PutUint32(keybt, keyint)
		vallen = uint32(len(value))
		binary.BigEndian.PutUint32(vallenbt, vallen)
		outbt = append(outbt, keybt...)
		outbt = append(outbt, vallenbt...)
		outbt = append(outbt, value...)
	}
	return outbt
}

func BytesToMapIntBytes(mdatastr []byte) sync.Map {
	var mdata sync.Map
	var key, vallen, startpos uint32
	for uint32(startpos)+8 <= uint32(len(mdatastr)) {
		key = binary.BigEndian.Uint32(mdatastr[startpos : startpos+4])
		vallen = binary.BigEndian.Uint32(mdatastr[startpos+4 : startpos+4+4])
		mdata.Store(uint32(key), BytesClone(mdatastr[startpos+4+4:startpos+4+4+vallen]))
		startpos += 4 + 4 + vallen
		if startpos >= uint32(len(mdatastr)) {
			break
		}
	}
	return mdata
}

func BytesToMapU32Bytes(mdatastr []byte) (mdata map[uint32][]byte) {
	mdata = make(map[uint32][]byte, 0)
	var key, vallen, startpos uint32
	for uint32(startpos)+8 <= uint32(len(mdatastr)) {
		key = binary.BigEndian.Uint32(mdatastr[startpos : startpos+4])
		vallen = binary.BigEndian.Uint32(mdatastr[startpos+4 : startpos+4+4])
		mdata[uint32(key)] = BytesClone(mdatastr[startpos+4+4 : startpos+4+4+vallen])
		startpos += 4 + 4 + vallen
		if startpos >= uint32(len(mdatastr)) {
			break
		}
	}
	return mdata
}

func CompressAndSave(orderf *OrderFile, segindinfo chan uint32, newsavedataf *os.File, oldboatfbufcurlen, oldboatffilepos, oldboatfbufsize *uint64, oldboatfbuf []byte) {
	zbuf := make([]byte, 1<<17)
	for true {
		segindi := <-segindinfo
		if segindi == ^uint32(0) {
			break
		}
		segind := uint32(segindi)

		segindblock, _ := orderf.cachemap.Load(segind)
		var zblock []byte
		var segindblock2 []byte
		if segindblock.([]byte)[len(segindblock.([]byte))-1]&1 == 1 {

			segindblock.([]byte)[len(segindblock.([]byte))-1] = segindblock.([]byte)[len(segindblock.([]byte))-1] & 0xFE
			segindblock2 = segindblock.([]byte)[:len(segindblock.([]byte))-8]
			if len(segindblock.([]byte)) == 0 {
				panic("length error 931")
			}
			if orderf.compresstype == 0 {
				zblock = ZipEncode(zbuf, segindblock2, 1)
			} else if orderf.compresstype == 1 {
				zblock = SnappyEncode(zbuf, segindblock2)
			} else if orderf.compresstype == 2 {
				zblock = Lz4Encode(zbuf, segindblock2)
			} else if orderf.compresstype == 3 {
				zblock = XzEncode(zbuf, segindblock2)
			} else if orderf.compresstype == 4 {
				zblock = FlateEncode(zbuf, segindblock2, 1)
			}
			orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+segind*11 : 24+segind*11+2*11])
			orderf.readfullboatmu.Lock()
			maybeoldfilespace := dataposlen
			var writepos uint64 = datapos
			if uint64(len(zblock)) > maybeoldfilespace || datapos == 0 {
				//free old file space: maybe first maybe refree
				isreleaseoldfilespace := false
				if dataposlen > 0 && datapos > 0 {
					dataposbt := make([]byte, 8)
					binary.BigEndian.PutUint64(dataposbt, datapos)
					startposlist, bstartposlist := orderf.spacelen_pos[uint32(dataposlen)]
					if bstartposlist {
						orderf.spacelen_pos[uint32(dataposlen)] = append(startposlist, dataposbt[3:]...)
					} else {
						orderf.spacelen_pos[uint32(dataposlen)] = dataposbt[3:]
					}
					isreleaseoldfilespace = true
				}
				if dataposlen == 0 || isreleaseoldfilespace || writepos == 0 {
					//new file space
					isfoundspacepos := false
					for deltalen := 0; deltalen < 2; deltalen++ {
						poslist, bposlist := orderf.spacelen_pos[uint32(len(zblock)+deltalen)]
						if bposlist {
							writepos = binary.BigEndian.Uint64(append([]byte{0, 0, 0}, poslist[len(poslist)-5:]...))
							if len(poslist)-5 >= 5 {
								orderf.spacelen_pos[uint32(len(zblock)+deltalen)] = poslist[:len(poslist)-5]
							} else {
								delete(orderf.spacelen_pos, uint32(len(zblock)+deltalen))
							}
							isfoundspacepos = true
							break
						}
					}
					if writepos == 0 || !isfoundspacepos {
						writepos = orderf.fakelastblockendpos
						orderf.fakelastblockendpos += uint64(len(zblock))
					}
				}
			}
			//fmt.Println(writepos, zblock)
			var wcnt int
			if writepos >= orderf.fileendwritebuffilepos {
				if orderf.fileendwritebufcurlen+uint64(len(zblock)) >= orderf.fileendwritebufsize {
					orderf.pathfile.Seek(int64(orderf.fileendwritebuffilepos), os.SEEK_SET)
					orderf.pathfile.Write(orderf.fileendwritebuf[:orderf.fileendwritebufcurlen])
					copy(orderf.fileendwritebuf[:], zblock[:])
					orderf.fileendwritebuffilepos += orderf.fileendwritebufcurlen
					orderf.fileendwritebufcurlen = uint64(len(zblock))
				} else {
					copy(orderf.fileendwritebuf[orderf.fileendwritebufcurlen:], zblock)
					orderf.fileendwritebufcurlen += uint64(len(zblock))
				}
			} else {
				/*
					if *oldboatfbufcurlen+8+4+uint64(len(zblock)) >= *oldboatfbufsize {
						newsavedataf.Seek(int64(*oldboatffilepos), os.SEEK_SET)
						newsavedataf.Write(oldboatfbuf[:*oldboatfbufcurlen])
						*oldboatffilepos += *oldboatfbufcurlen
						*oldboatfbufcurlen = 0
					}
					binary.BigEndian.PutUint64(oldboatfbuf[*oldboatfbufcurlen:*oldboatfbufcurlen+8], uint64(writepos))
					binary.BigEndian.PutUint32(oldboatfbuf[*oldboatfbufcurlen+8:*oldboatfbufcurlen+8+4], uint32(len(zblock)))
					copy(oldboatfbuf[*oldboatfbufcurlen+8+4:*oldboatfbufcurlen+8+4+uint64(len(zblock))], zblock)
					*oldboatfbufcurlen += 8 + 4 + uint64(len(zblock))
				*/
				writeposbt := make([]byte, 8)
				binary.BigEndian.PutUint64(writeposbt, writepos)
				newsavedataf.Write(writeposbt)
				zbocklenbt := make([]byte, 4)
				binary.BigEndian.PutUint32(zbocklenbt, uint32(len(zblock)))
				newsavedataf.Write(zbocklenbt)
				newsavedataf.Write(zblock)
			}
			orderf.readfullboatmu.Unlock()
			setBlockPos(orderf.shorttable[24+segind*11:24+segind*11+2*11], orderstart, orderlen, writepos, uint64(len(zblock)), 0)
			//fmt.Fprintln(ff, "orderstart6", segind, "writepos:", writepos, blockdatalen, "endpos:", orderf.lastblockendpos)
			//fmt.Fprintln(ff, "orderf.lastblockendpos2", orderf.lastblockendpos, segind, writepos, datapos, total1, total2)
			if len(zblock) == 0 {
				panic(orderf.path + ":data length is zero 732.")
			}

			if uint32(wcnt) != uint32(len(zblock)) {
				//panic(orderf.path+":write error 736.")
			}

			//fmt.Fprintln(ff, segind, orderstart, orderlen, writepos, uint64(needspace), zblock)
			orderf.isheadmodified = true
			//fmt.Println(orderf.path,"segment data move:", oldorderstart, oldorderstatlen, dataposold, dataposoldlen, orderstart, orderlen, writepos, len(zblock), orderf.path)
		}
	}
	zbuf = []byte{}
	orderf.CompressAndSavewg.Add(-1)
}

//true only for prepareflush
func (orderf *OrderFile) SetAutoFlush(autoflush bool) {
	orderf.autoflush = autoflush
}

func (orderf *OrderFile) PrepareFlush() {
	if orderf.prepareflush == false {
		//fmt.Println(orderf.path, "PrepareFlush")
		orderf.WaitBufMapEmpty()
		orderf.prepareflush = true
		<-orderf.prepareflushendch
	}
}

func (orderf *OrderFile) CompleteFlush() {
	//fmt.Println(orderf.path, "CompleteFlush start")
	orderf.prepareflushendch <- 1
	//fmt.Println(orderf.path, "CompleteFlush end")
}

func (orderf *OrderFile) EndFlushWait() {
	<-orderf.prepareflushendch
}

func DeletePrepareFlush(orderfilepath string) {
	//fmt.Println(orderfilepath, "DeletePrepareFlush")
	os.Remove(orderfilepath + ".headsaveok")
	os.Remove(orderfilepath + ".headsave")
	os.Remove(orderfilepath + ".bfreesave")
	os.Remove(orderfilepath + ".ffreesave")
	os.Remove(orderfilepath + ".newsavedata")
	os.Remove(orderfilepath + ".rmpushinsave")
	os.Remove(orderfilepath + ".rmpushinmem")
}

func BlockFlush(orderf *OrderFile) {
	//time.Sleep(100 * time.Millisecond)
	//pushmapempty := false
	pushmapempty := true
	for true {
		//save memory item
		orderf.markmu.Lock()
		markrmpushfilepos, markrmpushfileerr := orderf.markrmpushfile.Seek(0, os.SEEK_CUR)
		orderf.markmu.Unlock()
		if orderf.prepareflush && pushmapempty || orderf.autoflush == true && (orderf.isflushnow || sysidlemem.GetSysIdleMem() < orderf.sysminidlemem && time.Now().Unix()-orderf.presynctime > orderf.idlememflushinterval || time.Now().Unix()-orderf.presynctime > orderf.autoflushinterval) || markrmpushfileerr == nil && markrmpushfilepos > orderf.markrmpushfilemaxsize {
			bmemreach := false
			if sysidlemem.GetSysIdleMem() < orderf.sysminidlemem {
				bmemreach = true
			}
			orderf.ordermu.RLock()
			if orderf.bhavenewwrite {
				orderf.fileendwritebuffilepos = orderf.fakelastblockendpos
				if uint32(orderf.datablockcnt) != orderf.lastblockind+1 {
					panic(orderf.path + " Maybe RealPush have error.")
				}
				orderf.fileendwritebuf = make([]byte, orderf.fileendwritebufsize+1024)
				//orderf.ErrorRecord("F", []byte{}, nil)
				orderf.markmu.Lock()
				os.Remove(orderf.path + ".headsaveok")
				orderf.markrmpushfile.Sync()
				orderf.markrmpushfile.Close()
				os.Remove(orderf.path + ".rmpushinsave")
				//inmf, _ := os.Create(orderf.path + ".rmpushinmem")
				os.Rename(orderf.path+".rmpush", orderf.path+".rmpushinsave")
				orderf.markrmpushfile, _ = os.Create(orderf.path + ".rmpush")
				//intempcnt := 0
				//runpos = 2
				// if orderf.enablermpushlog {
				// 	cvtbuf := make([]byte, 8)
				// 	orderf.markrmmap.Range(func(key, val interface{}) bool {
				// 		keylen := len([]byte(key.(string)))
				// 		binary.BigEndian.PutUint32(cvtbuf, uint32(keylen)|(uint32(1)<<31))
				// 		orderf.markrmpushfile.Write(cvtbuf[:4])
				// 		orderf.markrmpushfile.Write([]byte(key.(string)))
				// 		inmf.Write(cvtbuf[:4])
				// 		inmf.Write([]byte(key.(string)))
				// 		return true
				// 	})
				// 	orderf.markpushmap.Range(func(key, val interface{}) bool {
				// 		keylen := len([]byte(key.(string)))
				// 		binary.BigEndian.PutUint32(cvtbuf, uint32(keylen))
				// 		orderf.markrmpushfile.Write(cvtbuf[:4])
				// 		orderf.markrmpushfile.Write([]byte(key.(string)))

				// 		inmf.Write(cvtbuf[:4])
				// 		inmf.Write([]byte(key.(string)))

				// 		binary.BigEndian.PutUint32(cvtbuf, uint32(0))
				// 		orderf.markrmpushfile.Write(cvtbuf[:4])

				// 		inmf.Write(cvtbuf[:4])

				// 		intempcnt += 1
				// 		return true
				// 	})
				// 	orderf.markrmpushmap.Range(func(key, val interface{}) bool {
				// 		keylen := len([]byte(key.(string)))
				// 		binary.BigEndian.PutUint32(cvtbuf, uint32(keylen))
				// 		orderf.markrmpushfile.Write(cvtbuf[:4])
				// 		orderf.markrmpushfile.Write([]byte(key.(string)))

				// 		inmf.Write(cvtbuf[:4])
				// 		inmf.Write([]byte(key.(string)))

				// 		vallen := len(val.([]byte))
				// 		binary.BigEndian.PutUint32(cvtbuf, uint32(vallen))
				// 		orderf.markrmpushfile.Write(cvtbuf[:4])
				// 		orderf.markrmpushfile.Write(val.([]byte))

				// 		inmf.Write(cvtbuf[:4])
				// 		inmf.Write(val.([]byte))
				// 		intempcnt += 1
				// 		return true
				// 	})
				// }
				orderf.markrmpushfile.Sync()
				//inmf.Close()
				orderf.markmu.Unlock()
				newsavedataf, _ := os.Create(orderf.path + ".newsavedata")
				segindchan := make(chan uint32, 1)
				cpunum := runtime.NumCPU()
				if cpunum > 12 {
					cpunum = 12
				}
				var oldboatfbufcurlen, oldboatffilepos, oldboatfbufsize uint64
				var oldboatfbuf []byte
				//oldboatfbufsize = 1 * 1024 * 1024
				//oldboatfbuf = make([]byte, oldboatfbufsize)
				for i := 0; i < cpunum; i++ {
					orderf.CompressAndSavewg.Add(1)
					go CompressAndSave(orderf, segindchan, newsavedataf, &oldboatfbufcurlen, &oldboatffilepos, &oldboatfbufsize, oldboatfbuf)
				}
				//fmt.Println(orderf.path, "blockflush create CompressAndSave ok")
				//test code start
				/*
					keys := []uint32{}
					orderf.cachemap.Range(func(key, val interface{}) bool {
						keys = append(keys, key.(uint32))
						return true
					})
					sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
					for _, segind := range keys {
						segindchan <- segind
					}
				*/
				//test code end
				//real code
				orderf.cachemap.Range(func(key, val interface{}) bool {
					if val.([]byte)[len(val.([]byte))-1]&1 == 1 {
						segindchan <- key.(uint32)
					}
					return true
				})
				//fmt.Println(orderf.path, "blockflush send index ok")
				for i := 0; i < cpunum; i++ {
					segindchan <- ^uint32(0)
				}
				//fmt.Println(orderf.path, "blockflush CompressAndSavewg wait")
				orderf.CompressAndSavewg.Wait()
				//fmt.Println(orderf.path, "blockflush CompressAndSavewg wait end")
				//fmt.Println(orderf.path, "blockflush orderf.ordermu.Lock 1")
				if oldboatfbufcurlen > 0 {
					newsavedataf.Seek(int64(oldboatffilepos), os.SEEK_SET)
					newsavedataf.Write(oldboatfbuf[:oldboatfbufcurlen])
					oldboatffilepos += oldboatfbufcurlen
					oldboatfbufcurlen = 0
				}
				if orderf.fileendwritebufcurlen > 0 {
					//orderf.pathfile.Seek(int64(orderf.fileendwritebuffilepos), os.SEEK_SET)
					//orderf.pathfile.Write(orderf.fileendwritebuf[:orderf.fileendwritebufcurlen])

					// cvtbuf := make([]byte, 8)
					// binary.BigEndian.PutUint64(cvtbuf, uint64(orderf.fileendwritebuffilepos))
					// newsavedataf.Write(cvtbuf[:8])
					// binary.BigEndian.PutUint32(cvtbuf, uint32(orderf.fileendwritebufcurlen))
					// newsavedataf.Write(cvtbuf[:4])
					// newsavedataf.Write(orderf.fileendwritebuf[:orderf.fileendwritebufcurlen])
					orderf.readfullboatmu.Lock()
					orderf.pathfile.Seek(int64(orderf.fileendwritebuffilepos), os.SEEK_SET)
					orderf.pathfile.Write(orderf.fileendwritebuf[:orderf.fileendwritebufcurlen])
					orderf.readfullboatmu.Unlock()

					orderf.fileendwritebuffilepos += orderf.fileendwritebufcurlen
					orderf.fileendwritebufcurlen = 0
				}
				orderf.readfullboatmu.Lock()
				orderf.pathfile.Sync()
				orderf.readfullboatmu.Unlock()
				newsavedataf.Sync()
				newsavedataf.Close()
				//fmt.Println(orderf.path, "blockflush orderf.ordermu.Lock 2")
				if orderf.fileendwritebuffilepos != orderf.fakelastblockendpos {
					fmt.Println(orderf.path, "fileendwritebuffilepos,lastblockendpos:", orderf.fileendwritebuffilepos, orderf.lastblockendpos, orderf.fakelastblockendpos)
					panic("file end position not equal error 946.")
				}
				//fmt.Println(orderf.path, "blockflush orderf.ordermu.Lock 3")

				//flush shorttable
				//if orderf.isheadmodified {

				binary.BigEndian.PutUint32(orderf.shorttable[6:10], uint32(orderf.datablockcnt))
				binary.BigEndian.PutUint32(orderf.shorttable[10:14], uint32(orderf.lastblockind))
				binary.BigEndian.PutUint32(orderf.shorttable[14:18], 0)
				binary.BigEndian.PutUint16(orderf.shorttable[18:20], uint16(orderf.totalcount>>32))
				binary.BigEndian.PutUint32(orderf.shorttable[20:24], uint32(orderf.totalcount&0xFFFFFFFF))
				var zh []byte
				if orderf.compresstype == 0 {
					zh = ZipEncode(nil, orderf.shorttable, 1)
				} else if orderf.compresstype == 1 {
					zh = SnappyEncode(nil, orderf.shorttable)
				} else if orderf.compresstype == 2 {
					zh = Lz4Encode(nil, orderf.shorttable)
				} else if orderf.compresstype == 3 {
					zh = XzEncode(nil, orderf.shorttable)
				} else if orderf.compresstype == 4 {
					zh = FlateEncode(nil, orderf.shorttable, 1)
				}
				if orderf.totalcount < 0 {
					panic(orderf.path + ":total count error 871.")
				}
				err := orderfiletool.WriteFile(orderf.path+".headsave", zh)
				if err != nil {
					fmt.Println(orderf.path, "head path:", orderf.path, err)
					panic(orderf.path + ":write head error 877.")
				}
				var zbfree []byte
				if orderf.compresstype == 0 {
					zbfree = ZipEncode(nil, MapU32BytesToBytes(orderf.releasespacels), 1)
				} else if orderf.compresstype == 1 {
					zbfree = SnappyEncode(nil, MapU32BytesToBytes(orderf.releasespacels))
				} else if orderf.compresstype == 2 {
					zbfree = Lz4Encode(nil, MapU32BytesToBytes(orderf.releasespacels))
				} else if orderf.compresstype == 3 {
					zbfree = XzEncode(nil, MapU32BytesToBytes(orderf.releasespacels))
				} else if orderf.compresstype == 4 {
					zbfree = FlateEncode(nil, MapU32BytesToBytes(orderf.releasespacels), 1)
				}
				err = orderfiletool.WriteFile(orderf.path+".bfreesave", zbfree)
				if err != nil {
					fmt.Println(orderf.path, "bfree path:", orderf.path, err)
					panic(orderf.path + ":write bfree error 884.")
				}
				zbfree = []byte{}
				var zffree []byte
				if orderf.compresstype == 0 {
					zffree = ZipEncode(nil, MapU32BytesToBytes(orderf.spacelen_pos), 1)
				} else if orderf.compresstype == 1 {
					zffree = SnappyEncode(nil, MapU32BytesToBytes(orderf.spacelen_pos))
				} else if orderf.compresstype == 2 {
					zffree = Lz4Encode(nil, MapU32BytesToBytes(orderf.spacelen_pos))
				} else if orderf.compresstype == 3 {
					zffree = XzEncode(nil, MapU32BytesToBytes(orderf.spacelen_pos))
				} else if orderf.compresstype == 4 {
					zffree = FlateEncode(nil, MapU32BytesToBytes(orderf.spacelen_pos), 1)
				}
				err = orderfiletool.WriteFile(orderf.path+".ffreesave", zffree)
				if err != nil {
					fmt.Println(orderf.path, "ffree path:", orderf.path, err)
					panic(orderf.path + ":write ffree error 884.")
				}
				zffree = []byte{}

				//}
				//fmt.Println(orderf.path, "blockflush orderf.ordermu.Lock 4")
				//if orderf.prepareflush != true {
				//exec.Command("sync")
				//}
				//time.Sleep(10 * time.Millisecond)
				orderfiletool.WriteFile(orderf.path+".headsaveok", []byte{0})
				//if orderf.prepareflush != true {
				//	exec.Command("sync")
				//}
				if orderf.prepareflush == true {
					orderf.prepareflushendch <- 1
					<-orderf.prepareflushendch
				}
				//write item data to orderfile
				tempcompref, tempcompreferr := os.OpenFile(orderf.path+".newsavedata", os.O_RDWR, 0666)
				if tempcompreferr == nil {
					endpos, _ := tempcompref.Seek(0, os.SEEK_END)
					if endpos > 0 {
						tempcompref.Seek(0, os.SEEK_SET)
						readfrom := int64(0)
						curi := int64(0)
						readlen := int64(0)
						//runpos = 27
						timestart := time.Now().Unix()
						opcnt := 0
						for i := int64(0); i < endpos; {
							readlen = int64(len(orderf.fileendwritebuf)) - readfrom
							if i+readlen > endpos {
								readlen = endpos - i
							}
							_, readerr := tempcompref.Read(orderf.fileendwritebuf[readfrom : readfrom+readlen])
							if readerr != nil {
								orderf.isopen = false
								orderf.markrmpushfile.Sync()
								orderfiletool.WriteFile("lasterror.log", []byte(orderf.path+" "+strconv.FormatInt(int64(i), 10)+" "+strconv.FormatInt(int64(readfrom), 10)+" "+strconv.FormatInt(int64(readlen), 10)))
								panic(orderf.path + " read newsavedata have error.")
								break
							}
							curi = int64(0)
							for true {
								opcnt += 1
								if opcnt%5000 == 0 {
									fmt.Println(orderf.path, "opcnt:", opcnt)
								}
								if curi+12 > readfrom+readlen {
									copy(orderf.fileendwritebuf, orderf.fileendwritebuf[curi:readfrom+readlen])
									readfrom = readfrom + readlen - curi
									break
								}
								writepos := int64(binary.BigEndian.Uint64(orderf.fileendwritebuf[curi : curi+8]))
								datalen := int64(binary.BigEndian.Uint32(orderf.fileendwritebuf[curi+8 : curi+8+4]))
								if curi+8+4+datalen > readfrom+readlen {
									copy(orderf.fileendwritebuf, orderf.fileendwritebuf[curi:readfrom+readlen])
									readfrom = readfrom + readlen - curi
									break
								}

								//posf, _ := os.Create(orderf.path + ".posf." + strconv.FormatInt(int64(writepos), 10))
								//posf.Write(orderf.fileendwritebuf[curi+8+4 : curi+8+4+datalen])
								//posf.Close()
								orderf.readfullboatmu.Lock()
								orderf.pathfile.Seek(writepos, os.SEEK_SET)
								wcnt, werr := orderf.pathfile.Write(orderf.fileendwritebuf[curi+8+4 : curi+8+4+datalen])
								if werr != nil {
									fmt.Println(orderf.path, " Write error:", werr, wcnt)
									panic("write error.")
								}
								//fmt.Println("1writepos writelen", writepos, datalen, orderf.fileendwritebuf[curi+8+4:curi+8+4+datalen])
								orderf.readfullboatmu.Unlock()
								curi += 8 + 4 + datalen
							}
							i += readlen
						}
						if time.Now().Unix()-timestart > 10 {
							fmt.Println(orderf.path, "save back time", time.Now().Unix()-timestart)
						}
					}
					tempcompref.Close()
				}

				orderf.readfullboatmu.Lock()
				orderf.pathfile.Sync()
				endpos2, endpos2e := orderf.pathfile.Seek(0, os.SEEK_END)
				if endpos2e != nil {
					fmt.Println("endpos2e", endpos2e, endpos2)
					panic(orderf.path + " pathfile seek end error")
				}
				//fmt.Println(orderf.path, "blockflush end at ", endpos2, orderf.fileendwritebuffilepos, orderf.fakelastblockendpos)
				orderf.fakelastblockendpos = uint64(endpos2)
				dataflenbt := make([]byte, 8)
				binary.BigEndian.PutUint64(dataflenbt, uint64(endpos2))
				_, er := orderf.pathfile.Seek(0, os.SEEK_SET)
				if er == nil {
					orderf.pathfile.Write(dataflenbt[3:])
				} else {
					panic("seek error 1346")
				}
				orderf.readfullboatmu.Unlock()
				orderf.isheadmodified = false
				//time.Sleep(10 * time.Millisecond)
				//if orderf.isheadmodified{
				os.Remove(orderf.path + ".head.backup")
				os.Rename(orderf.path+".head", orderf.path+".head.backup")
				os.Rename(orderf.path+".headsave", orderf.path+".head")

				os.Remove(orderf.path + ".bfree.backup")
				os.Rename(orderf.path+".bfree", orderf.path+".bfree.backup")
				os.Rename(orderf.path+".bfreesave", orderf.path+".bfree")

				os.Remove(orderf.path + ".ffree.backup")
				os.Rename(orderf.path+".ffree", orderf.path+".ffree.backup")
				os.Rename(orderf.path+".ffreesave", orderf.path+".ffree")
				//}

				os.Remove(orderf.path + ".rmpushinsave")
				os.Remove(orderf.path + ".rmpushinmem")
				os.Remove(orderf.path + ".newsavedata")
				//if orderf.prepareflush != true {
				//	exec.Command("sync")
				//}
				orderf.fileendwritebuf = []byte{}
				orderf.lastcompleteorderset = orderf.lastcompleteorderset[:0]
				orderf.lastblockendpos = orderf.fakelastblockendpos
				orderf.bhavenewwrite = false
			} else {
				if orderf.prepareflush == true {
					orderf.prepareflushendch <- 1
					<-orderf.prepareflushendch
				}
			}

			//release cachemap
			//fmt.Println(orderf.path, "orderf.toatalreusecount/orderf.totalcachecount", orderf.toatalreusecount, orderf.totalcachecount)
			var clearpercent float64
			if bmemreach {
				clearpercent = 0.7
			} else {
				clearpercent = 0.1
			}
			startusecnt := orderf.totalcachecount / 2
			orderf.cachemap.Range(func(key, val interface{}) bool {
				celldata := val.([]byte)
				if celldata[len(celldata)-1]&1 != 1 && key.(uint32) < orderf.lastblockind {
					cntval := binary.BigEndian.Uint64(celldata[len(celldata)-8:]) >> 1
					if orderf.totalcachecount <= 0 {
						atomic.StoreInt64(&orderf.totalcachecount, 1)
					}
					if cntval <= uint64(float64(orderf.maxreusecount)*clearpercent) {
						atomic.AddInt64(&orderf.toatalreusecount, -int64(cntval))
						atomic.AddInt64(&orderf.totalcachecount, -1)
						orderf.readfullboatmu.Lock()
						orderf.cachemap.Delete(key)
						orderf.readfullboatmu.Unlock()
					}
					startusecnt -= 1
					if startusecnt <= 0 {
						//return false
					}
				}
				return true
			})

			orderf.ordermu.RUnlock()

			orderf.markmu.Lock()
			orderf.markrmpushfile.Sync()
			orderf.markmu.Unlock()

			if orderf.prepareflush == true {
				orderf.prepareflushendch <- 1
				orderf.prepareflush = false
			}
			os.Remove(orderf.path + ".headsaveok")

			orderf.presynctime = time.Now().Unix()
			if orderf.isflushnow {
				orderf.flushnowch <- 1
				orderf.isflushnow = false
			}

			if orderf.splitcnt > 0 {
				if orderf.splittype == 0 {
					curkey := []byte{}
					totalcnt := orderf.Count()
					percnt := totalcnt/int64(orderf.splitcnt) + 1
					var curdb *OrderFile
					cursptdiri := 1
					curcnt := int64(0)
					for true {
						nextkey, bnextkey := orderf.NextKey(curkey)
						if !bnextkey {
							break
						}
						if curdb == nil || curcnt >= percnt {
							if curdb != nil {
								curdb.Close()
							}
							curdb, _ = OpenOrderFile(orderfiletool.FilePathReplaceDir(orderf.path, strconv.FormatInt(int64(cursptdiri), 10)), orderf.compresstype, orderf.fixkeylen, orderf.fixkeyendbt)
							curdb.SetFlushInterval(99999999999)
							cursptdiri += 1
							curcnt = 0
						}
						curdb.RealPush(nextkey)
						curcnt += 1
						curkey = nextkey
					}
					if curdb != nil {
						curdb.Close()
					}
				} else if orderf.splittype == 1 {
					modval_db := map[uint32]*OrderFile{}
					for modvali := uint32(0); modvali < uint32(orderf.splitcnt); modvali++ {
						modval_db[modvali+1], _ = OpenOrderFile(orderfiletool.FilePathReplaceDir(orderf.path, strconv.FormatInt(int64(modvali+1), 10)), orderf.compresstype, orderf.fixkeylen, orderf.fixkeyendbt)
						modval_db[modvali+1].SetFlushInterval(999999999999)
					}
					curkey := []byte{}
					for true {
						nextkey, bnextkey := orderf.NextKey(curkey)
						if !bnextkey {
							break
						}
						db, bdb := modval_db[(binary.BigEndian.Uint32(nextkey[:4])%uint32(orderf.splitcnt))+1]
						if !bdb {
							panic("unknow error.")
						}
						db.Push(nextkey)
						curkey = nextkey
					}
					for _, val := range modval_db {
						val.Close()
					}
				} else if orderf.splittype == 2 {
					modval_db := map[uint64]*OrderFile{}
					for modvali := uint64(0); modvali < uint64(orderf.splitcnt); modvali++ {
						modval_db[modvali+1], _ = OpenOrderFile(orderfiletool.FilePathReplaceDir(orderf.path, strconv.FormatInt(int64(modvali+1), 10)), orderf.compresstype, orderf.fixkeylen, orderf.fixkeyendbt)
						modval_db[modvali+1].SetFlushInterval(999999999999)
					}
					curkey := []byte{}
					for true {
						nextkey, bnextkey := orderf.NextKey(curkey)
						if !bnextkey {
							break
						}
						db, bdb := modval_db[(binary.BigEndian.Uint64(nextkey[:8])%uint64(orderf.splitcnt))+1]
						if !bdb {
							panic("unknow error.")
						}
						db.Push(nextkey)
						curkey = nextkey
					}
					for _, val := range modval_db {
						val.Close()
					}
				}
				orderf.splitcnt = 0
			}

			//fmt.Println(orderf.path, "flush end")
			if orderf.isquit && orderf.isflushnow == false {
				orderf.isopen = false
				//runpos = 40
				//runch <- 1
				break
			}

		} else {
			// //item alloc memory struct
			// cnt := 0
			// orderf.markmu.Lock()
			// orderf.markrmmap.Range(func(key, val interface{}) bool {
			// 	orderf.RealRm(append([]byte(key.(string)), orderf.fixkeyendbt...))
			// 	cnt += 1
			// 	orderf.markrmmap.Delete(key.(string))
			// 	if cnt == 10 {
			// 		return false
			// 	} else {
			// 		return true
			// 	}
			// })
			// if cnt < 10 {
			// 	orderf.markpushmap.Range(func(key, val interface{}) bool {
			// 		orderf.RealPush([]byte(key.(string)))
			// 		cnt += 1
			// 		orderf.markpushmap.Delete(key.(string))
			// 		if cnt == 10 {
			// 			return false
			// 		} else {
			// 			return true
			// 		}
			// 	})
			// }
			// if cnt < 10 {
			// 	orderf.markrmpushmap.Range(func(key, val interface{}) bool {
			// 		orderf.RealRmPush(append([]byte(key.(string)), orderf.fixkeyendbt...), val.([]byte))
			// 		cnt += 1
			// 		orderf.markrmpushmap.Delete(key.(string))
			// 		if cnt == 10 {
			// 			return false
			// 		} else {
			// 			return true
			// 		}
			// 	})
			// }
			// orderf.markmu.Unlock()
			// if cnt == 0 {
			// 	pushmapempty = true
			// } else {
			// 	pushmapempty = false
			// }
			// if cnt >= 10 {
			// 	continue
			// } else {
			// 	bsaveone := false
			// 	if orderf.autoflush == true && len(orderf.lastcompleteorderset) > 512 {
			// 		segind := orderf.lastcompleteorderset[0]
			// 		copy(orderf.lastcompleteorderset, orderf.lastcompleteorderset[1:])
			// 		orderf.lastcompleteorderset = orderf.lastcompleteorderset[:len(orderf.lastcompleteorderset)-1]
			// 		if segind < orderf.lastblockind {
			// 			segindblock, bsegindblock := orderf.cachemap.Load(segind)
			// 			if bsegindblock && segindblock.([]byte)[len(segindblock.([]byte))-1]&1 == 1 {
			// 				segindblock2 := segindblock.([]byte)
			// 				orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+segind*11 : 24+segind*11+2*11])
			// 				if datapos == 0 || datapos >= orderf.lastblockendpos {
			// 					if len(segindblock2)-8 > 0 {
			// 						segindblock2[len(segindblock2)-1] = segindblock2[len(segindblock2)-1] & 0xFE
			// 						segindblock2 = segindblock2[:len(segindblock2)-8]
			// 						var zblock []byte
			// 						if orderf.compresstype == 0 {
			// 							zblock = ZipEncode(nil, segindblock2, 1)
			// 						} else if orderf.compresstype == 1 {
			// 							zblock = SnappyEncode(nil, segindblock2)
			// 						} else if orderf.compresstype == 2 {
			// 							zblock = Lz4Encode(nil, segindblock2)
			// 						} else if orderf.compresstype == 3 {
			// 							zblock = XzEncode(nil, segindblock2)
			// 						} else if orderf.compresstype == 4 {
			// 							zblock = FlateEncode(nil, segindblock2, 1)
			// 						}
			// 						var writepos uint64
			// 						if uint64(len(zblock)) > dataposlen {
			// 							if dataposlen > 0 {
			// 								dataposbt := make([]byte, 8)
			// 								binary.BigEndian.PutUint64(dataposbt, datapos)
			// 								startposlist, bstartposlist := orderf.spacelen_pos[uint32(dataposlen)]
			// 								if bstartposlist {
			// 									orderf.spacelen_pos[uint32(dataposlen)] = append(startposlist, dataposbt[1:]...)
			// 								} else {
			// 									orderf.spacelen_pos[uint32(dataposlen)] = dataposbt[1:]
			// 								}
			// 							}
			// 							writepos = orderf.fakelastblockendpos
			// 							orderf.fakelastblockendpos += uint64(len(zblock))
			// 						} else {
			// 							//use old file space
			// 							writepos = datapos
			// 						}
			// 						orderf.readfullboatmu.Lock()
			// 						orderf.pathfile.Seek(int64(writepos), os.SEEK_SET)
			// 						orderf.pathfile.Write(zblock)
			// 						orderf.readfullboatmu.Unlock()
			// 						//fmt.Println("2writepos writelen", writepos, len(zblock), zblock)
			// 						setBlockPos(orderf.shorttable[24+segind*11:24+segind*11+2*11], orderstart, orderlen, writepos, uint64(len(zblock)), 0)
			// 						bsaveone = true
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}
			// 	if !bsaveone {
			// 		time.Sleep(333 * time.Millisecond)
			// 	}
			// }

			bsaveone := false
			if orderf.autoflush == true && len(orderf.lastcompleteorderset) > 512 {
				segind := orderf.lastcompleteorderset[0]
				copy(orderf.lastcompleteorderset, orderf.lastcompleteorderset[1:])
				orderf.lastcompleteorderset = orderf.lastcompleteorderset[:len(orderf.lastcompleteorderset)-1]
				if segind < orderf.lastblockind {
					segindblock, bsegindblock := orderf.cachemap.Load(segind)
					if bsegindblock && segindblock.([]byte)[len(segindblock.([]byte))-1]&1 == 1 {
						segindblock2 := segindblock.([]byte)
						orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+segind*11 : 24+segind*11+2*11])
						if datapos == 0 || datapos >= orderf.lastblockendpos {
							if len(segindblock2)-8 > 0 {
								segindblock2[len(segindblock2)-1] = segindblock2[len(segindblock2)-1] & 0xFE
								segindblock2 = segindblock2[:len(segindblock2)-8]
								var zblock []byte
								if orderf.compresstype == 0 {
									zblock = ZipEncode(nil, segindblock2, 1)
								} else if orderf.compresstype == 1 {
									zblock = SnappyEncode(nil, segindblock2)
								} else if orderf.compresstype == 2 {
									zblock = Lz4Encode(nil, segindblock2)
								} else if orderf.compresstype == 3 {
									zblock = XzEncode(nil, segindblock2)
								} else if orderf.compresstype == 4 {
									zblock = FlateEncode(nil, segindblock2, 1)
								}
								var writepos uint64
								if uint64(len(zblock)) > dataposlen {
									if dataposlen > 0 {
										dataposbt := make([]byte, 8)
										binary.BigEndian.PutUint64(dataposbt, datapos)
										startposlist, bstartposlist := orderf.spacelen_pos[uint32(dataposlen)]
										if bstartposlist {
											orderf.spacelen_pos[uint32(dataposlen)] = append(startposlist, dataposbt[1:]...)
										} else {
											orderf.spacelen_pos[uint32(dataposlen)] = dataposbt[1:]
										}
									}
									writepos = orderf.fakelastblockendpos
									orderf.fakelastblockendpos += uint64(len(zblock))
								} else {
									//use old file space
									writepos = datapos
								}
								orderf.readfullboatmu.Lock()
								orderf.pathfile.Seek(int64(writepos), os.SEEK_SET)
								orderf.pathfile.Write(zblock)
								orderf.readfullboatmu.Unlock()
								//fmt.Println("2writepos writelen", writepos, len(zblock), zblock)
								setBlockPos(orderf.shorttable[24+segind*11:24+segind*11+2*11], orderstart, orderlen, writepos, uint64(len(zblock)), 0)
								bsaveone = true
							}
						}
					}
				}
			}
			if !bsaveone {
				time.Sleep(333 * time.Millisecond)
			}
		}
	}
}

func (orderf *OrderFile) WaitBufMapEmpty() {
	// for true {
	// 	if orderf.isopen == false {
	// 		return
	// 	}
	// 	var b1, b2, b3 bool = true, true, true
	// 	orderf.markrmmap.Range(func(key, val interface{}) bool {
	// 		b1 = false
	// 		return false
	// 	})
	// 	orderf.markpushmap.Range(func(key, val interface{}) bool {
	// 		b2 = false
	// 		return false
	// 	})
	// 	orderf.markrmpushmap.Range(func(key, val interface{}) bool {
	// 		b3 = false
	// 		return false
	// 	})
	// 	if b1 && b2 && b3 {
	// 		return
	// 	}
	// 	time.Sleep(10 * time.Millisecond)
	// }
}

func (orderf *OrderFile) IsBufMapEmpty() bool {
	// var b1, b2, b3 bool = true, true, true
	// orderf.markmu.Lock()
	// orderf.markrmmap.Range(func(key, val interface{}) bool {
	// 	b1 = false
	// 	return false
	// })
	// orderf.markpushmap.Range(func(key, val interface{}) bool {
	// 	b2 = false
	// 	return false
	// })
	// orderf.markrmpushmap.Range(func(key, val interface{}) bool {
	// 	b3 = false
	// 	return false
	// })
	// orderf.markmu.Unlock()
	// return b1 && b2 && b3

	return true
}

func DBCompress(orderdbpath string, replaceoldfile bool, tailspace int) bool {
	OrderFileClear(orderdbpath + ".compress")
	shorttable, err := orderfiletool.ReadFile(orderdbpath + ".head")
	if err != nil {
		return false
	}
	pathfile, err := os.OpenFile(orderdbpath, os.O_RDWR, 0666)
	if err != nil {
		return false
	}
	comprbt := make([]byte, 8)
	pathfile.Read(comprbt)
	var compresstype int
	compresstype = int(comprbt[5])
	if compresstype == 0 {
		shorttable = ZipDecode(nil, shorttable)
	} else if compresstype == 1 {
		shorttable = SnappyDecode(nil, shorttable)
	} else if compresstype == 2 {
		shorttable = Lz4Decode(nil, shorttable)
	} else if compresstype == 3 {
		shorttable = XzDecode(nil, shorttable)
	} else if compresstype == 4 {
		shorttable = FlateDecode(nil, shorttable)
	}
	datablockcnt := binary.BigEndian.Uint32(shorttable[6:10])

	pathfile2, err := os.Create(orderdbpath + ".compress")
	if err != nil {
		return false
	}
	pathfile2.Write(comprbt[:5])
	pathfile2.Write([]byte{byte(compresstype)})
	predatastart := uint64(6)
	predata := make([]byte, 0)
	tailspacebt := make([]byte, tailspace)
	for i := uint32(0); i < datablockcnt; i++ {
		orderstart, orderlen, datapos2, dataposlen2, _ := getBlockPos(shorttable[24+i*11 : 24+i*11+2*11])
		_, err := pathfile.Seek(int64(datapos2), os.SEEK_SET)
		if err != nil {
			fmt.Println(err, "compress 1738")
			panic("read error")
		}
		blockbt := make([]byte, dataposlen2)
		rdcnt, err := pathfile.Read(blockbt)
		if err != nil || uint64(rdcnt) != uint64(len(blockbt)) {
			panic("data error 1049.")
		}
		predata = append(predata, blockbt...)
		predata = append(predata, tailspacebt...)
		setBlockPos(shorttable[24+i*11:24+i*11+2*11], orderstart, orderlen, predatastart, uint64(len(blockbt)), 0)
		predatastart += uint64(len(blockbt) + tailspace)
		if len(predata) > 52428800 {
			wcnt, err := pathfile2.Write(predata)
			if err != nil || wcnt != len(predata) {
				panic("write data error 1058.")
			}
			predata = make([]byte, 0)
		}
	}
	if len(predata) > 0 {
		wcnt, err := pathfile2.Write(predata)
		if err != nil || wcnt != len(predata) {
			panic("write data error 1066.")
		}
		predata = make([]byte, 0)
	}
	endpos2, endpos2e := pathfile2.Seek(0, os.SEEK_END)
	if endpos2e != nil {
		panic("seek error 1626")
	}
	binary.BigEndian.PutUint64(comprbt, uint64(endpos2))
	pathfile2.Seek(0, os.SEEK_SET)
	pathfile2.Write(comprbt[3:])
	pathfile.Close()
	pathfile2.Close()
	var zsh []byte
	if compresstype == 0 {
		zsh = ZipEncode(nil, shorttable, 1)
	} else if compresstype == 1 {
		zsh = SnappyEncode(nil, shorttable)
	} else if compresstype == 2 {
		zsh = Lz4Encode(nil, shorttable)
	} else if compresstype == 3 {
		zsh = XzEncode(nil, shorttable)
	} else if compresstype == 4 {
		zsh = FlateEncode(nil, shorttable, 1)
	}
	orderfiletool.WriteFile(orderdbpath+".compress.head", zsh)
	freebt, _ := orderfiletool.ReadFile(orderdbpath + ".bfree")
	orderfiletool.WriteFile(orderdbpath+".compress.bfree", freebt)
	orderfiletool.WriteFile(orderdbpath+".compress.ffree", []byte{})

	CopyFile(orderdbpath+".rmpush", orderdbpath+".compress.rmpush")
	CopyFile(orderdbpath+".rmpushinsave", orderdbpath+".compress.rmpushinsave")
	CopyFile(orderdbpath+".rmpushinmem", orderdbpath+".compress.rmpushinmem")
	CopyFile(orderdbpath+".opt", orderdbpath+".compress.opt")

	if replaceoldfile {
		MoveToOld(orderdbpath)

		os.Rename(orderdbpath+".compress", orderdbpath)
		os.Rename(orderdbpath+".compress.head", orderdbpath+".head")
		os.Rename(orderdbpath+".compress.bfree", orderdbpath+".bfree")
		os.Rename(orderdbpath+".compress.rmpush", orderdbpath+".rmpush")
		os.Rename(orderdbpath+".compress.rmpushinsave", orderdbpath+".rmpushinsave")
		os.Rename(orderdbpath+".compress.rmpushinmem", orderdbpath+".rmpushinmem")
		os.Rename(orderdbpath+".compress.opt", orderdbpath+".opt")
	}

	return true
}

func (orderf *OrderFile) Split(splitcnt, splittype int) bool {
	if !orderf.isopen {
		return false
	}
	orderf.splitcnt = splitcnt
	orderf.splittype = splittype
	orderf.isflushnow = true
	<-orderf.flushnowch
	orderf.isflushnow = false
	return true
}

func (orderf *OrderFile) Flush() bool {
	if !orderf.isopen {
		return false
	}
	orderf.isflushnow = true
	<-orderf.flushnowch
	orderf.isflushnow = false
	return true
}

func (orderf *OrderFile) Close() bool {
	if !orderf.isopen {
		return false
	}
	//orderf.ErrorRecord("C", []byte(orderf.path), nil)
	//fmt.Println(orderf.path, "close 1")
	orderf.autoflush = true
	orderf.isflushnow = true
	orderf.isquit = true
	<-orderf.flushnowch
	orderf.cachemap.Range(func(key, val interface{}) bool {
		orderf.cachemap.Delete(key)
		return true
	})
	// orderf.markrmmap.Range(func(key, val interface{}) bool {
	// 	orderf.markrmmap.Delete(key)
	// 	return true
	// })
	// orderf.markpushmap.Range(func(key, val interface{}) bool {
	// 	orderf.markpushmap.Delete(key)
	// 	return true
	// })
	// orderf.markrmpushmap.Range(func(key, val interface{}) bool {
	// 	orderf.markpushmap.Delete(key)
	// 	return true
	// })
	for key, _ := range orderf.releasespacels {
		delete(orderf.releasespacels, key)
	}
	for key, _ := range orderf.spacelen_pos {
		delete(orderf.spacelen_pos, key)
	}
	orderf.shorttable = []byte{}
	orderf.extranbuf = []byte{}
	os.Remove(orderf.path + ".beopen")
	orderf.markrmpushfile.Close()
	orderf.pathfile.Close()
	forbidReopenMap.Delete(orderf.pathlockid)
	orderf.pathlockid = ""
	filelock.Unlock(orderf.flockid)
	orderf.flockid = 0

	//fmt.Println(orderf.path, "close 6")
	// t1 := time.Now().Unix()
	//runtime.GC()
	// if time.Now().Unix()-t1 > 5 {
	// 	panic("GC error!")
	// }
	// fmt.Println(orderf.path, "close 7")
	return true
}

func (orderf *OrderFile) freeSpaceSize(blockdata []byte) uint32 {
	return orderf.cellsize - uint32(len(blockdata))
}

func (orderf *OrderFile) fillToCellSize(data []byte) []byte {
	freelen := orderf.freelen(uint32(len(data)))
	return append(data, make([]byte, freelen)...)
}

func (orderf *OrderFile) cellWriteNoZip(cellindex uint32, orderstart, orderlen, datapos, dataposlen, orderlenchange uint64, bendpos, btrunc bool, celldata []byte) {
	orderf.bhavenewwrite = true
	setBlockPos(orderf.shorttable[24+cellindex*11:24+cellindex*11+2*11], orderstart, orderlen, datapos, dataposlen, orderlenchange)
	orderf.cachemap.Store(cellindex, orderf.setValTouch(celldata, 0, true))
	atomic.AddInt64(&orderf.totalcachecount, 1)
}

func v4simreadFullBoatNoZipcheck(objsetname string, ch chan int, pos, postime *int64, runstr *string) {
	//fmt.Println("getBlockPosV2Check start:", runstr)
	for true {
		select {
		case <-time.After(time.Second * 45):
			fmt.Println("v4simreadFullBoatNoZipcheck timeout:", objsetname)
			panic("v4simreadFullBoatNoZipcheck running time too long at position " + strconv.FormatInt(*pos, 10) + " postime:" + strconv.FormatInt(*postime, 10) + " nowtime:" + strconv.FormatInt(time.Now().Local().Unix(), 10) + " str:" + *runstr)
		case <-ch:
			return
		}
	}
}

func (orderf *OrderFile) readFullBoatNoZip(boatoffsetid uint64) []byte {
	// runch := make(chan int, 0)
	// runpos := int64(0)
	// runpostime := time.Now().Local().Unix()
	// var runstr string
	// go v4simreadFullBoatNoZipcheck(string(""), runch, &runpos, &runpostime, &runstr)

	blockind := orderf.quickFindOrderInd(boatoffsetid)
	orderstart, orderlen, datapos, dataposlen, truncblock := getBlockPos(orderf.shorttable[24+blockind*11 : 24+blockind*11+2*11])

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, "readFullBoatNoZip 1621 boatoffsetid", boatoffsetid, "orderstart, orderlen, datapos, dataposlen, truncblock", orderstart, orderlen, datapos, dataposlen, truncblock)
			panic(r)
		}
	}()
	orderf.readfullboatmu.Lock()
	celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
	oldcntval := uint64(0)
	var ucelldata []byte
	if !bcelldata2 {
		celldata := make([]byte, dataposlen)
		orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
		readcnt, err := orderf.pathfile.Read(celldata)
		if err != nil || readcnt != len(celldata) {
			fmt.Println(orderf.path, "boatoffsetid", boatoffsetid, "readcnt:", readcnt, "len(celldata)", len(celldata), celldata, err, dataposlen, datapos)
			panic(orderf.path + ":readFullBoatNoZip read error 1203.")
		}
		if orderf.compresstype == 0 {
			ucelldata = ZipDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 1 {
			ucelldata = SnappyDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 2 {
			ucelldata = Lz4Decode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 3 {
			ucelldata = XzDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 4 {
			ucelldata = FlateDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		}
		if len(ucelldata) == 0 || boatoffsetid-orderstart >= uint64(len(ucelldata)) {
			fmt.Println(orderf.path, "readerror:", orderf.compresstype, boatoffsetid, orderstart, orderlen, datapos, dataposlen, len(ucelldata), ucelldata, celldata)
			panic(orderf.path + ":length error 1211.")
		}
		atomic.AddInt64(&orderf.totalcachecount, 1)
		orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, false))
	} else {
		oldcntval = getOldCount(celldata2.([]byte))
		binary.BigEndian.PutUint64(celldata2.([]byte)[len(celldata2.([]byte))-8:], ((oldcntval+1)<<1)|(uint64(celldata2.([]byte)[len(celldata2.([]byte))-1])&1))
		ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
		atomic.AddInt64(&orderf.toatalreusecount, 1)
	}
	orderf.readfullboatmu.Unlock()
	if boatoffsetid-orderstart > uint64(len(ucelldata)) {
		fmt.Println(orderf.path, "boatoffsetid:", boatoffsetid, "blockind:", blockind, "orderstart:", orderstart, "orderlen:", orderlen, "datapos:", datapos, "dataposlen:", dataposlen, "truncblock:", truncblock, "bcelldata2", bcelldata2, "ucelldata:", ucelldata)
		panic(orderf.path + ":error! maybe memory low 1224.")
	}
	//fmt.Println(orderf.path, "boatoffsetid:", boatoffsetid, "blockind:", blockind, "ucelldata:", len(ucelldata), ucelldata)
	_, charlen, chardelta, childlen, bnextsuc := boatHasNext(ucelldata[boatoffsetid-orderstart:])
	if bnextsuc == false {
		fmt.Println(orderf.path, "boatoffsetid:", boatoffsetid, "blockind:", blockind, orderstart, orderlen, datapos, dataposlen, truncblock, "ucelldata:", len(ucelldata), ucelldata)
		panic(orderf.path + ":boat check error 1230.")
	}
	//boat := make([]byte, charlen+chardelta+childlen)
	//copy(boat, ucelldata[boatoffsetid-orderstart:boatoffsetid-orderstart+charlen+chardelta+childlen])
	boat := ucelldata[boatoffsetid-orderstart : boatoffsetid-orderstart+charlen+chardelta+childlen]
	return boat
}

func (orderf *OrderFile) upToCellSize(dataposlen uint32) uint32 {
	if dataposlen%uint32(orderf.cellsize) != 0 {
		return dataposlen + (uint32(orderf.cellsize) - dataposlen%uint32(orderf.cellsize))
	} else {
		return dataposlen
	}
}

func (orderf *OrderFile) freelen(dataposlen uint32) uint32 {
	return orderf.cellsize - dataposlen%orderf.cellsize
}

func (orderf *OrderFile) writeLastBlockNoZip(boat []byte) uint64 {
	orderf.bhavenewwrite = true
	if orderf.brecordzeroboat {
		var releaseboatpos uint64
		for i := 0; i < 2; i++ {
			spacebt, bspacebt := orderf.releasespacels[uint32(len(boat)+i)]
			if bspacebt {
				bt8 := make([]byte, 8)
				copy(bt8[3:], spacebt[len(spacebt)-5:])
				releaseboatpos = binary.BigEndian.Uint64(bt8)
				if len(spacebt)-5 >= 5 {
					orderf.releasespacels[uint32(len(boat)+i)] = spacebt[:len(spacebt)-5]
				} else {
					delete(orderf.releasespacels, uint32(len(boat)+i))
				}
				break
			}
		}
		if releaseboatpos != 0 {
			blockind := orderf.quickFindOrderInd(releaseboatpos)
			orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+blockind*11 : 24+blockind*11+2*11])
			celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
			oldcntval := uint64(0)
			var ucelldata []byte
			var bspacepass bool
			if !bcelldata2 {
				if orderf.benablewritelastloadfile == false {
					bspacepass = false
				} else {
					celldata := make([]byte, dataposlen)
					orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
					readcnt, err := orderf.pathfile.Read(celldata)
					if err != nil || readcnt != len(celldata) {
						panic(orderf.path + ":writeLastBlockNoZip read error 1289.")
					}
					if orderf.compresstype == 0 {
						ucelldata = ZipDecode(orderf.extranbuf, celldata)
						ucelldata = BytesClone(ucelldata)
					} else if orderf.compresstype == 1 {
						ucelldata = SnappyDecode(orderf.extranbuf, celldata)
						ucelldata = BytesClone(ucelldata)
					} else if orderf.compresstype == 2 {
						ucelldata = Lz4Decode(orderf.extranbuf, celldata)
						ucelldata = BytesClone(ucelldata)
					} else if orderf.compresstype == 3 {
						ucelldata = XzDecode(orderf.extranbuf, celldata)
						ucelldata = BytesClone(ucelldata)
					} else if orderf.compresstype == 4 {
						ucelldata = FlateDecode(orderf.extranbuf, celldata)
						ucelldata = BytesClone(ucelldata)
					}
					if len(ucelldata) == 0 || releaseboatpos-orderstart >= uint64(len(ucelldata)) {
						fmt.Println(orderf.path, "readerror:", releaseboatpos, orderstart, orderlen, datapos, dataposlen, len(ucelldata), ucelldata, celldata)
						panic(orderf.path + ":length error 1297.")
					}
					bspacepass = true
					atomic.AddInt64(&orderf.totalcachecount, 1)
				}
			} else {
				oldcntval = getOldCount(celldata2.([]byte))
				ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
				bspacepass = true
			}
			if bspacepass {
				copy(ucelldata[releaseboatpos-orderstart:], boat)
				orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, true))
				return releaseboatpos
			}
		}
	}
	var retboatoffset uint64
	orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+orderf.lastblockind*11 : 24+orderf.lastblockind*11+2*11])
	if orderlen+uint64(len(boat)) <= 3*uint64(orderf.cellsize) {
		celldata2, bcelldata2 := orderf.cachemap.Load(uint32(orderf.lastblockind))
		oldcntval := uint64(0)
		var ucelldata []byte
		if !bcelldata2 {
			celldata := make([]byte, dataposlen)
			orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
			readcnt, err := orderf.pathfile.Read(celldata)
			if err != nil || readcnt != len(celldata) {
				panic(orderf.path + ":writeLastBlockNoZip read error 1329.")
			}
			if orderf.compresstype == 0 {
				ucelldata = ZipDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 1 {
				ucelldata = SnappyDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 2 {
				ucelldata = Lz4Decode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 3 {
				ucelldata = XzDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 4 {
				ucelldata = FlateDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			}
			if len(ucelldata) == 0 {
				orderf.printSortTable()
				fmt.Println("blockind,orderstart,orderlen,datapos,dataposlen", orderf.lastblockind, orderstart, orderlen, datapos, dataposlen, celldata, ucelldata)
				panic(orderf.path + ":length error 1336.")
			}
			atomic.AddInt64(&orderf.totalcachecount, 1)
		} else {
			oldcntval = getOldCount(celldata2.([]byte))
			ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
		}
		ucelldata = append(ucelldata, boat...)
		//fmt.Println("ucelldata3", ucelldata)
		setBlockPos(orderf.shorttable[24+orderf.lastblockind*11:24+orderf.lastblockind*11+2*11], orderstart, orderlen+uint64(len(boat)), datapos, dataposlen, 1)
		newucelldata := orderf.setValTouch(ucelldata, oldcntval, true)
		orderf.cachemap.Store(uint32(orderf.lastblockind), newucelldata)
		//fmt.Println(orderf.path, "writecell3", blockind, lastbt)
		orderf.lastblockorderend = orderstart + orderlen + uint64(len(boat))
		retboatoffset = orderstart + orderlen
	} else {
		if len(orderf.lastcompleteorderset) == 0 || orderf.lastcompleteorderset[len(orderf.lastcompleteorderset)-1] < uint32(orderf.lastblockind) {
			if len(orderf.lastcompleteorderset) < 1024 {
				orderf.lastcompleteorderset = append(orderf.lastcompleteorderset, uint32(orderf.lastblockind))
			}
		}
		if 24+(orderf.datablockcnt)*11+64 > uint32(len(orderf.shorttable)) {
			orderf.shorttable = append(orderf.shorttable, make([]byte, orderf.cellsize)...)
		}
		//fmt.Println(orderf.path, "cellwritenozip:", uint32(orderf.datablockcnt-1), orderstart+orderlen, uint32(len(boat)))
		orderf.cellWriteNoZip(uint32(orderf.lastblockind+1), orderstart+orderlen, uint64(len(boat)), 0, 0, 1, true, false, boat)
		//fmt.Println(orderf.path, "writelastpos2", orderstart+orderlen, uint64(len(boat)), orderf.datablockcnt-1)
		orderf.lastblockorderend = orderstart + orderlen + uint64(len(boat))
		retboatoffset = orderstart + orderlen
		orderf.datablockcnt++
		orderf.lastblockind++
	}
	return retboatoffset
}

func (orderf *OrderFile) quickFindOrderInd(boatoffset uint64) int64 {
	start := int64(0)
	end := int64(orderf.datablockcnt) - 1
	cur := (start + end) / 2
	for true {
		orderstart, orderlen, _, _, _ := getBlockPos(orderf.shorttable[24+cur*11 : 24+cur*11+2*11])
		if boatoffset < orderstart {
			end = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				if cur2-1 >= start {
					cur = cur2 - 1
					end = cur
				} else {
					panic(orderf.path + ":boatoffset error 1374.")
				}
			} else {
				cur = cur2
			}
		} else if boatoffset >= orderstart+orderlen {
			start = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				if cur2+1 <= end {
					cur = cur2 + 1
					start = cur
				} else {
					fmt.Println(orderf.path, "boatoffset", boatoffset, orderstart, orderlen, orderf.datablockcnt, cur, start, end, orderf.shorttable)
					panic(orderf.path + ":boatoffset error 1391.")
					return -1
				}
			} else {
				cur = cur2
			}
		} else if boatoffset != orderstart+orderlen {
			return cur
		}
	}
	//fmt.Println(orderf.path,"boatoffset", boatoffset)
	panic(orderf.path + ":boatoffset error 1402.")
	return -1
}

func (orderf *OrderFile) zeroOldBoatNoZip(boatoffsetid, oldboatlen uint64) bool {
	orderf.bhavenewwrite = true
	blockind := orderf.quickFindOrderInd(boatoffsetid)
	orderstart, _, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+blockind*11 : 24+blockind*11+2*11])
	celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
	oldcntval := uint64(0)
	var ucelldata []byte
	if !bcelldata2 {
		celldata := make([]byte, dataposlen)
		orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
		readcnt, err := orderf.pathfile.Read(celldata)
		if err != nil || readcnt != len(celldata) {
			panic(orderf.path + ":writeLastBlockNoZip read error 1420.")
		}
		if orderf.compresstype == 0 {
			ucelldata = ZipDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 1 {
			ucelldata = SnappyDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 2 {
			ucelldata = Lz4Decode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 3 {
			ucelldata = XzDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		} else if orderf.compresstype == 4 {
			ucelldata = FlateDecode(orderf.extranbuf, celldata)
			ucelldata = BytesClone(ucelldata)
		}
		if len(ucelldata) == 0 {
			panic(orderf.path + ":length error 1427.")
		}
		atomic.AddInt64(&orderf.totalcachecount, 1)
	} else {
		oldcntval = getOldCount(celldata2.([]byte))
		ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
	}
	for i := uint64(0); i < oldboatlen; i++ {
		ucelldata[boatoffsetid-orderstart+i] = 0
	}

	orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, true))
	//fmt.Println(orderf.path, "writecell4", blockind, tucelldata)
	return true
}

var releasecnt int

func (orderf *OrderFile) writeOldBoatNoZip(boatoffset, oldboatlen uint64, boat []byte) uint64 {
	blockind := orderf.quickFindOrderInd(boatoffset)
	orderstart, orderlen, datapos, dataposlen, _ := getBlockPos(orderf.shorttable[24+blockind*11 : 24+blockind*11+2*11])
	orderf.bhavenewwrite = true
	if boatoffset+oldboatlen == orderstart+orderlen && uint32(blockind) == orderf.lastblockind && boatoffset+oldboatlen == orderf.lastblockorderend {
		celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
		oldcntval := uint64(0)
		var ucelldata []byte
		if !bcelldata2 {
			celldata := make([]byte, dataposlen)
			orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
			readcnt, err := orderf.pathfile.Read(celldata)
			if err != nil || readcnt != len(celldata) {
				panic(orderf.path + ":writeLastBlockNoZip read error 1474.")
			}
			if orderf.compresstype == 0 {
				ucelldata = ZipDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 1 {
				ucelldata = SnappyDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 2 {
				ucelldata = Lz4Decode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 3 {
				ucelldata = XzDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			} else if orderf.compresstype == 4 {
				ucelldata = FlateDecode(orderf.extranbuf, celldata)
				ucelldata = BytesClone(ucelldata)
			}
			if len(ucelldata) == 0 {
				panic(orderf.path + ":writeOldBoatNoZip length error 1481.")
			}
			atomic.AddInt64(&orderf.totalcachecount, 1)
		} else {
			oldcntval = getOldCount(celldata2.([]byte))
			ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
		}
		ucelldata = append(ucelldata[:boatoffset-orderstart], boat...)
		//fmt.Println("ucelldata5", ucelldata)
		orderlen = orderlen + (uint64(len(boat)) - oldboatlen)
		if orderlen > 0 && orderlen < (1<<17) {
			setBlockPos(orderf.shorttable[24+blockind*11:24+blockind*11+2*11], orderstart, orderlen, datapos, dataposlen, 1)
			orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, true))
			orderf.lastblockorderend = orderstart + orderlen
		} else if orderlen == 0 {
			var orderstart2, orderlen2 uint64
			orderstart2, orderlen2, _, _, _ = getBlockPos(orderf.shorttable[24+(blockind-1)*11 : 24+(blockind-1)*11+2*11])
			setBlockPos(orderf.shorttable[24+blockind*11:24+blockind*11+2*11], orderstart, 0, 0, 0, 0)

			orderf.lastblockind = uint32(blockind) - 1
			orderf.datablockcnt = uint32(blockind)
			for curnexti := int64(0); curnexti < 11; curnexti++ {
				orderf.shorttable[24+(blockind+1)*11+curnexti] = 0
			}
			orderf.lastblockorderend = orderstart2 + orderlen2
			orderf.cachemap.Delete(orderstart)
			if dataposlen > 0 {
				dataposbt := make([]byte, 8)
				binary.BigEndian.PutUint64(dataposbt, datapos)
				startposlist, bstartposlist := orderf.spacelen_pos[uint32(dataposlen)]
				if bstartposlist {
					orderf.spacelen_pos[uint32(dataposlen)] = append(startposlist, dataposbt[3:]...)
				} else {
					orderf.spacelen_pos[uint32(dataposlen)] = dataposbt[3:]
				}
			}
		} else {
			fmt.Println("blockind,orderstart, orderlen, datapos, dataposlen", blockind, orderstart, orderlen, datapos, dataposlen)
			panic(orderf.path + " unknow error")
		}
	} else {
		if oldboatlen < uint64(len(boat)) {
			orderf.zeroOldBoatNoZip(boatoffset, oldboatlen)
			rl := orderf.writeLastBlockNoZip(boat)
			if rl != boatoffset {
				if oldboatlen > 0 && boatoffset != 0 && orderf.brecordzeroboat {
					spacebt2 := make([]byte, 8)
					binary.BigEndian.PutUint64(spacebt2, uint64(boatoffset))
					//fmt.Println(orderf.path,"zerospace:", boatoffset, oldboatlen)
					spacebt, bspacebt := orderf.releasespacels[uint32(oldboatlen)]
					//fmt.Println(orderf.path,"spacebt2:", spacebt2, uint32(oldboatlen))
					if bspacebt {
						orderf.releasespacels[uint32(oldboatlen)] = append(spacebt, spacebt2[3:]...)
					} else {
						orderf.releasespacels[uint32(oldboatlen)] = spacebt2[3:]
					}
				}
			}
			boatoffset = rl
		} else if len(boat) > 0 {
			celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
			oldcntval := uint64(0)
			var ucelldata []byte
			if !bcelldata2 {
				celldata := make([]byte, dataposlen)
				orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
				readcnt, err := orderf.pathfile.Read(celldata)
				if err != nil || readcnt != len(celldata) {
					panic(orderf.path + ":writeLastBlockNoZip read error 1509.")
				}
				if orderf.compresstype == 0 {
					ucelldata = ZipDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 1 {
					ucelldata = SnappyDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 2 {
					ucelldata = Lz4Decode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 3 {
					ucelldata = XzDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 4 {
					ucelldata = FlateDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				}
				if len(ucelldata) == 0 {
					panic(orderf.path + ":length error 1516.")
				}
				atomic.AddInt64(&orderf.totalcachecount, 1)
			} else {
				oldcntval = getOldCount(celldata2.([]byte))
				ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
			}
			copy(ucelldata[boatoffset-orderstart:], boat)
			for i := uint64(0); i < oldboatlen-uint64(len(boat)); i++ {
				ucelldata[boatoffset-orderstart+uint64(len(boat))+i] = 0
			}
			orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, true))
			//fmt.Println(orderf.path, "writecell6", blockind, ldfsglgh)
		} else {
			celldata2, bcelldata2 := orderf.cachemap.Load(uint32(blockind))
			oldcntval := uint64(0)
			var ucelldata []byte
			if !bcelldata2 {
				celldata := make([]byte, dataposlen)
				orderf.pathfile.Seek(int64(datapos), os.SEEK_SET)
				readcnt, err := orderf.pathfile.Read(celldata)
				if err != nil || readcnt != len(celldata) {
					panic(orderf.path + ":writeLastBlockNoZip read error 1420.")
				}
				if orderf.compresstype == 0 {
					ucelldata = ZipDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 1 {
					ucelldata = SnappyDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 2 {
					ucelldata = Lz4Decode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 3 {
					ucelldata = XzDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				} else if orderf.compresstype == 4 {
					ucelldata = FlateDecode(orderf.extranbuf, celldata)
					ucelldata = BytesClone(ucelldata)
				}
				if len(ucelldata) == 0 {
					panic(orderf.path + ":length error 1427.")
				}
				atomic.AddInt64(&orderf.totalcachecount, 1)
			} else {
				oldcntval = getOldCount(celldata2.([]byte))
				ucelldata = celldata2.([]byte)[:len(celldata2.([]byte))-8]
			}
			for i := uint64(0); i < oldboatlen; i++ {
				ucelldata[boatoffset-orderstart+i] = 0
			}
			if oldboatlen > 0 && boatoffset != 0 && orderf.brecordzeroboat {
				spacebt2 := make([]byte, 8)
				binary.BigEndian.PutUint64(spacebt2, uint64(boatoffset))
				//fmt.Println(orderf.path,"zerospace:", boatoffset, oldboatlen)
				spacebt, bspacebt := orderf.releasespacels[uint32(oldboatlen)]
				//fmt.Println(orderf.path,"spacebt2:", spacebt2, uint32(oldboatlen))
				if bspacebt {
					orderf.releasespacels[uint32(oldboatlen)] = append(spacebt, spacebt2[3:]...)
				} else {
					orderf.releasespacels[uint32(oldboatlen)] = spacebt2[3:]
				}
			}
			orderf.cachemap.Store(uint32(blockind), orderf.setValTouch(ucelldata, oldcntval, true))
		}

	}
	return boatoffset
}

func (orderf *OrderFile) nextFillKey(boat, sentence []byte, sentencecuri int) ([]byte, bool) {
	boatcharlen, boatcharoffset := boatCharLen(boat)

	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}
	//fmt.Println(orderf.path,"nextFillKey:", samecnt, boatcharlen, samecnt, sentencecuri, sentence, boat)
	if samecnt == boatcharlen || samecnt > 0 || sentencecuri == len(sentence) {
		if boat[0]&1 == 1 {
			if sentencecuri == len(sentence) {
				if getBoatChildLen(boat) == 0 {
					if samecnt != boatcharlen {
						sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
						sentencecuri += int(boatcharlen - samecnt)
						return sentence, true
					} else {
						return sentence, true
					}
				} else if samecnt == boatcharlen {
					if samecnt != boatcharlen {
						sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
						sentencecuri += int(boatcharlen - samecnt)
						return sentence, true
					} else {
						return sentence, true
					}
				}
			}
		}

		var foundind int = -1
		if getBoatChildLen(boat) > 0 && samecnt == boatcharlen && sentencecuri < len(sentence) {
			foundind, _, _ = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
		} else if getBoatChildLen(boat) == 1+6 && sentencecuri == len(sentence) { //one
			foundind = 0
		}
		//fmt.Println(orderf.path,"foundind", foundind, sentence[1:], sentencecuri, sentence, boat)

		if foundind != -1 {
			if sentencecuri == len(sentence) && samecnt != boatcharlen {
				sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
				sentencecuri += int(boatcharlen - samecnt)
			}
			makeoffsetbt := make([]byte, 8)
			copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
			nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
			wordtypeval2 := nextoffset2 & 1
			switch wordtypeval2 {
			case 0:
				firstchr := byte(nextoffset2 >> 40)
				if sentencecuri != len(sentence) {
					sentencecuri++
				} else {
					sentence = append(sentence, firstchr)
					sentencecuri += 1
				}
				nextoffset2id := (nextoffset2 & 0xFFFFFFFFFF) >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				if sentencecuri <= len(sentence) || getBoatChildLen(subboat) == 0 || getBoatChildLen(subboat) == 1+6 && (subboat[0]&1) == 0 {
					return orderf.nextFillKey(subboat, sentence, sentencecuri)
				} else {
					return []byte{}, false
				}
			case 1:
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				if sentencecuri == len(sentence) {
					sentence = append(sentence, v3btn[2:2+v3btnlen]...)
					sentencecuri += int(v3btnlen)
					return sentence, true
				} else {
					remainlen := len(sentence) - sentencecuri
					if remainlen <= int(v3btnlen) {
						if bytes.Compare(sentence[sentencecuri:], v3btn[2:2+remainlen]) == 0 {
							sentence = append(sentence, v3btn[2+remainlen:2+v3btnlen]...)
							sentencecuri += int(v3btnlen) - remainlen
							return sentence, true
						}
					}
					return []byte{}, false
				}
			}
		}
	}
	return []byte{}, false
}

func (orderf *OrderFile) FillKey(key []byte) ([]byte, bool) {
	if !orderf.isopen {
		return []byte{}, false
	}
	// orderf.markmu.RLock()
	// _, keyvok := orderf.markrmmap.Load(string(key))
	// if keyvok {
	// 	orderf.markmu.RUnlock()
	// 	return nil, false
	// }
	// keyv, keyvok := orderf.markrmpushmap.Load(string(key))
	// if keyvok {
	// 	orderf.markmu.RUnlock()
	// 	return BytesCombine(append(key, orderf.fixkeyendbt...), keyv.([]byte)), true
	// }
	// orderf.markmu.RUnlock()
	return orderf.RealFill(BytesCombine(key, orderf.fixkeyendbt))
}

func (orderf *OrderFile) RealFill(key []byte) ([]byte, bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, key, "FillKey 2092", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()

	orderf.ordermu.RLock()

	//runpos = 2
	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	//runpos = 3
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	//runpos = 4
	rootboattype := rootboatoffset & 1
	var result []byte
	var bresult bool
	//runpos = 5
	if rootboattype == 0 {
		if len(key) == 0 && rootboat[0]&1 != 0 {
			bresult = true
		} else {
			//runpos = 6
			result, bresult = orderf.nextFillKey(rootboat, key, 0)
			//runpos = 7
		}
	}
	//runpos = 8
	orderf.ordermu.RUnlock()
	//runpos = 9
	//runch <- 1

	return result, bresult
}

func (orderf *OrderFile) nextKeyExists(boat, sentence []byte, sentencecuri int) bool {
	boatcharlen, boatcharoffset := boatCharLen(boat)

	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}
	//fmt.Println(orderf.path,"nextFillKey:", samecnt, boatcharlen, samecnt, sentencecuri, sentence, boat)
	if samecnt == boatcharlen || samecnt > 0 || sentencecuri == len(sentence) {
		if boat[0]&1 == 1 {
			if sentencecuri == len(sentence) {
				if getBoatChildLen(boat) == 0 {
					return true
				} else if samecnt == boatcharlen {
					return true
				}
			}
		}

		var foundind int = -1
		if getBoatChildLen(boat) > 0 && samecnt == boatcharlen && sentencecuri < len(sentence) {
			foundind, _, _ = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
		} else if getBoatChildLen(boat) == 1+6 && sentencecuri == len(sentence) { //one
			foundind = 0
		}

		if foundind != -1 && sentencecuri+1 == len(sentence) && orderf.fixkeylen == sentencecuri+1 {
			return true
		}

		if foundind != -1 {
			makeoffsetbt := make([]byte, 8)
			copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
			nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
			wordtypeval2 := nextoffset2 & 1
			switch wordtypeval2 {
			case 0:
				if sentencecuri != len(sentence) {
					sentencecuri++
				}
				nextoffset2id := (nextoffset2 & 0xFFFFFFFFFF) >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				if sentencecuri <= len(sentence) || getBoatChildLen(subboat) == 0 || getBoatChildLen(subboat) == 1+6 && (subboat[0]&1) == 0 {
					bfill := orderf.nextKeyExists(subboat, sentence, sentencecuri)
					return bfill
				} else {
					return false
				}
			case 1:
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				if sentencecuri == len(sentence) {
					return true
				} else {
					remainlen := len(sentence) - sentencecuri
					if remainlen <= int(v3btnlen) {
						if bytes.Compare(sentence[sentencecuri:], v3btn[2 : 2+remainlen][:len(sentence[sentencecuri:])]) == 0 {
							return true
						}
					}
					return false
				}
			}
		}
	}
	return false
}

func (orderf *OrderFile) KeyExists(key []byte) bool {
	if !orderf.isopen {
		return false
	}

	if orderf.fixkeylen > 0 && len(key) != orderf.fixkeylen {
		return false
	}

	// orderf.markmu.RLock()
	// _, keyvok := orderf.markrmpushmap.Load(string(key))
	// if keyvok {
	// 	orderf.markmu.RUnlock()
	// 	return true
	// }
	// orderf.markmu.RUnlock()
	return orderf.RealKeyExists(BytesCombine(key, orderf.fixkeyendbt))
}

func (orderf *OrderFile) RealKeyExists(key []byte) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, key, "FillKey 2092", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()

	if orderf.fixkeylen > 0 && len(key) != orderf.fixkeylen {
		return false
	}

	orderf.ordermu.RLock()
	rootboatinfo := make([]byte, 8)
	copy(rootboatinfo[2:], orderf.shorttable[0:6])
	rootboatoffset := binary.BigEndian.Uint64(rootboatinfo)
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	rootboattype := rootboatoffset & 1
	var bresult bool
	if rootboattype == 0 {
		if len(key) == 0 && rootboat[0]&1 != 0 {
			bresult = true
		} else {
			bresult = orderf.nextKeyExists(rootboat, key, 0)
		}
	}
	orderf.ordermu.RUnlock()

	return bresult
}

func (orderf *OrderFile) nextRandGet(boat, sentence []byte, sentencecuri int) (rtkey []byte) {
	boatcharlen, boatcharoffset := boatCharLen(boat)

	sentence = append(sentence, boat[boatcharoffset:boatcharoffset+boatcharlen]...)
	sentencecuri = len(sentence)

	randcnt := 0
	bboathaveval := false
	if boat[0]&1 == 1 {
		randcnt += 1
		bboathaveval = true
	}
	if uint64(len(boat)) > boatcharoffset+boatcharlen {
		randcnt += int(boat[boatcharoffset+boatcharlen]) + 1
	}
	if randcnt > 0 {
		randrl := rand.Int() % randcnt
		var foundind int = -1
		if bboathaveval {
			if randrl == 0 {
				return sentence
			} else {
				foundind = randrl - 1
			}
		} else {
			foundind = randrl
		}

		if foundind != -1 {
			makeoffsetbt := make([]byte, 8)
			copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
			nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
			wordtypeval2 := nextoffset2 & 1
			switch wordtypeval2 {
			case 0:
				firstchr := byte(nextoffset2 >> 40)
				sentence = append(sentence, firstchr)
				sentencecuri = len(sentence)
				nextoffset2id := (nextoffset2 & 0xFFFFFFFFFF) >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				return orderf.nextRandGet(subboat, sentence, sentencecuri)
			case 1: //value end
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				sentence = append(sentence, v3btn[2:2+v3btnlen]...)
				return sentence
			}
		}
	} else {
		//fmt.Println("boat byte:")
		//fmt.Println(boat)
	}
	return nil
}

//when key is hash that have high rand
func (orderf *OrderFile) RandGet() (rtkey []byte) {
	if !orderf.isopen {
		return nil
	}
	// randval := rand.Int63() % (1 + orderf.Count())
	// if randval == 0 {
	// 	orderf.markmu.RLock()
	// 	var rtval []byte = nil
	// 	orderf.markrmpushmap.Range(func(key, val interface{}) bool {
	// 		rtkey = []byte(key.(string))
	// 		rtval = val.([]byte)
	// 		return false
	// 	})
	// 	orderf.markmu.RUnlock()
	// 	if rtkey != nil {
	// 		return append(rtkey, rtval...)
	// 	}
	// }
	return orderf.RealRandGet()
}

func (orderf *OrderFile) RealRandGet() (rtkey []byte) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, "RealRandGet 2743", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()

	orderf.ordermu.RLock()

	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	rootboattype := rootboatoffset & 1
	var result []byte = nil
	if rootboattype == 0 {
		if rootboat[0]&1 != 0 {
			randval := rand.Int63() % orderf.Count()
			if randval == 0 {
				result = []byte{}
			} else {
				result = orderf.nextRandGet(rootboat, []byte{}, 0)
			}
		} else {
			result = orderf.nextRandGet(rootboat, []byte{}, 0)
		}
	}
	orderf.ordermu.RUnlock()
	return result
}

func (orderf *OrderFile) nextMaxMatch(boat, sentence []byte, sentencecuri int, wordmavec *[]uint32) {
	boatcharlen, boatcharoffset := boatCharLen(boat)

	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}
	if samecnt == boatcharlen || samecnt > 0 || sentencecuri == len(sentence) {
		if boat[0]&1 == 1 && samecnt == boatcharlen {
			*wordmavec = append(*wordmavec, uint32(sentencecuri))
		}
		var foundind int = -1
		if getBoatChildLen(boat) > 0 && samecnt == boatcharlen && sentencecuri < len(sentence) {
			foundind, _, _ = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
		} else if getBoatChildLen(boat) == 1+6 && sentencecuri == len(sentence) { //one
			foundind = 0
		}

		if foundind != -1 {
			makeoffsetbt := make([]byte, 8)
			copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
			nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
			wordtypeval2 := nextoffset2 & 1
			switch wordtypeval2 {
			case 0:
				if sentencecuri != len(sentence) {
					sentencecuri++
				}
				nextoffset2id := (nextoffset2 & 0xFFFFFFFFFF) >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				if sentencecuri < len(sentence) || getBoatChildLen(subboat) == 0 || getBoatChildLen(subboat) == 1+6 && (subboat[0]&1) == 0 {
					orderf.nextMaxMatch(subboat, sentence, sentencecuri, wordmavec)
					return
				} else {
					return
				}
			case 1:
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				if sentencecuri+int(v3btnlen) <= len(sentence) && bytes.Compare(sentence[sentencecuri:sentencecuri+int(v3btnlen)], v3btn[2:2+v3btnlen]) == 0 {
					*wordmavec = append(*wordmavec, uint32(sentencecuri+int(v3btnlen)))
				} else {
					return
				}
			}
		}
	}
	return
}

func (orderf *OrderFile) MaxMatch(sentence []byte) (wordmavec []uint32) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, "MaxMatch 2821", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()

	orderf.ordermu.RLock()

	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	rootboattype := rootboatoffset & 1
	if rootboattype == 0 {
		if rootboat[0]&1 != 0 {
			wordmavec = append(wordmavec, 0)
		}
		orderf.nextMaxMatch(rootboat, sentence, 0, &wordmavec)
	}
	orderf.ordermu.RUnlock()
	return wordmavec
}

func (orderf *OrderFile) nextPushValue3(val3u32 uint64, word []byte, wordcuri int) (offset uint64, offsetischange bool) {
	val3u32bt := make([]byte, 8)
	binary.BigEndian.PutUint64(val3u32bt, val3u32)
	val3len := int(val3u32bt[7] >> 1)
	matchcnt := 0
	for si := 0; si < val3len; si++ {
		if wordcuri+si == len(word) {
			break
		}
		if val3u32bt[2+si] != word[wordcuri+si] {
			break
		} else {
			matchcnt++
		}
	}

	if matchcnt == val3len && len(word)-wordcuri == matchcnt {
		return val3u32, false
	}

	if matchcnt == 0 {
		//fmt.Println(orderf.path,"val3u32bt:", val3u32bt, wordcuri, word)
		panic(orderf.path + ":matchcnt is 0 error 1641.")
	}
	orderf.totalcount += 1
	orderf.bhavenewwrite = true

	val3u32btfirstchar := val3u32bt[2]
	va3reboatbt, _ := orderf.buildLastWordMem(val3u32bt[2:2+matchcnt], 0, 0)
	va3reboatbt[0] = va3reboatbt[0] &^ 1

	var var3renainid uint64 = 0
	var var3renainidbtfirstchar byte
	if matchcnt > 0 && matchcnt < val3len {
		var3renainidbt := make([]byte, 8)
		var3renaincnt := val3len - matchcnt
		for i := 0; i < var3renaincnt; i++ {
			var3renainidbt[2+i] = val3u32bt[2+matchcnt+i]
		}
		var3renainidbt[7] = byte(var3renaincnt<<1 | 1)
		var3renainid = binary.BigEndian.Uint64(var3renainidbt)
		var3renainidbtfirstchar = var3renainidbt[2]
	} else if matchcnt == val3len {
		va3reboatbt[0] |= 1
	}

	wordlastcnt := len(word) - wordcuri - matchcnt
	var wordlastmemid uint64
	var wordlastmemfirstchar byte
	if wordlastcnt > 0 {
		if wordlastcnt > 5 {
			if orderf.fixkeyendbt != nil {
				orderf.fixkeylen = bytes.Index(word, orderf.fixkeyendbt)
				if orderf.fixkeylen == -1 {
					orderf.fixkeylen = 0
				} else {
					orderf.fixkeylen += len(orderf.fixkeyendbt)
				}
			}
			wordlastmem, _ := orderf.buildLastWordMem(word, wordcuri+matchcnt, orderf.fixkeylen-wordcuri+matchcnt)
			var wordlastboatoffset uint64
			wordlastboatoffset = orderf.writeLastBlockNoZip(wordlastmem)
			//check4Zero(wordlastmem)
			wordlastmemfirstchar = word[wordcuri+matchcnt]
			wordlastmemid = uint64(wordlastmemfirstchar)<<40 | uint64(wordlastboatoffset)<<1 | 0
		} else if wordlastcnt > 0 {
			w3bt := make([]byte, 8)
			for wi := 0; wi < wordlastcnt; wi++ {
				w3bt[2+wi] = word[wordcuri+matchcnt+wi]
			}
			w3bt[7] = byte(wordlastcnt)<<1 | 1

			wordlastmemid = binary.BigEndian.Uint64(w3bt)
			wordlastmemfirstchar = w3bt[2]
		}
	} else {
		va3reboatbt[0] |= 1
	}

	if wordlastmemid != 0 && var3renainid != 0 {
		va3reboatbt[0] |= 4
		va3reboatbt = append(va3reboatbt, byte(1))
		if wordlastmemfirstchar > var3renainidbtfirstchar {
			var3renainidbt := make([]byte, 8)
			binary.BigEndian.PutUint64(var3renainidbt, var3renainid)
			va3reboatbt = append(va3reboatbt, var3renainidbt[2:]...)
			wordlastmemidbt := make([]byte, 8)
			binary.BigEndian.PutUint64(wordlastmemidbt, wordlastmemid)
			va3reboatbt = append(va3reboatbt, wordlastmemidbt[2:]...)
		} else if wordlastmemfirstchar < var3renainidbtfirstchar {
			wordlastmemidbt := make([]byte, 8)
			binary.BigEndian.PutUint64(wordlastmemidbt, wordlastmemid)
			va3reboatbt = append(va3reboatbt, wordlastmemidbt[2:]...)
			var3renainidbt := make([]byte, 8)
			binary.BigEndian.PutUint64(var3renainidbt, var3renainid)
			va3reboatbt = append(va3reboatbt, var3renainidbt[2:]...)
		} else {
			panic(orderf.path + ":value has same first value error 1714.")
		}
	} else if var3renainid != 0 {
		va3reboatbt[0] |= 4
		va3reboatbt = append(va3reboatbt, byte(0))
		var3renainidbt := make([]byte, 8)
		binary.BigEndian.PutUint64(var3renainidbt, var3renainid)
		va3reboatbt = append(va3reboatbt, var3renainidbt[2:]...)
	} else if wordlastmemid != 0 {
		va3reboatbt[0] |= 4
		va3reboatbt = append(va3reboatbt, byte(0))
		wordlastmemidbt := make([]byte, 8)
		binary.BigEndian.PutUint64(wordlastmemidbt, wordlastmemid)
		va3reboatbt = append(va3reboatbt, wordlastmemidbt[2:]...)
	}
	var va3reboatbtoffset uint64
	va3reboatbtoffset = orderf.writeLastBlockNoZip(va3reboatbt)
	//check4Zero(va3reboatbt)
	return uint64(val3u32btfirstchar)<<40 | uint64(va3reboatbtoffset)<<1 | 0, true
}

func (orderf *OrderFile) nextPushKey(boatoffset uint64, word []byte, wordcuri int) (reboatoffset uint64, offsetischange bool) {
	boatoffsetid := boatoffset >> 1
	var boat []byte
	boat = orderf.readFullBoatNoZip(boatoffsetid)
	boatstartlen := len(boat)
	boatcharlen, boatcharoffset := boatCharLen(boat)
	var samecnt uint64
	for samecnt < boatcharlen && wordcuri < len(word) && boat[boatcharoffset+samecnt] == word[wordcuri] {
		samecnt++
		wordcuri++
	}

	if wordcuri == len(word) && boatcharlen == samecnt {
		if boat[0]&1 == 1 {
			return boatoffsetid, false
		} else {
			boat[0] = boat[0] | 1
			var boatoffsetid2 uint64
			boatoffsetid2 = orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
			orderf.totalcount += 1
			orderf.bhavenewwrite = true
			if boatoffsetid2 != boatoffsetid {
				return boatoffsetid2, true
			} else {
				return boatoffsetid, false
			}
		}
	}

	var wordind, mofpos, mofcmpval int = -1, -1, -1
	if getBoatChildLen(boat) > 0 && samecnt == boatcharlen {
		wordind, mofpos, mofcmpval = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], word, wordcuri)
	}
	//fmt.Println(orderf.path,"nextPushKey1", samecnt, wordcuri, word)
	//fmt.Println(orderf.path,"nextPushKey2", samecnt, len(boat), boat)
	if wordind != -1 {
		wordfirstchar := word[wordcuri]
		nextoffset := binary.BigEndian.Uint64(append(make([]byte, 2), boat[boatcharoffset+boatcharlen+1+uint64(wordind)*6:boatcharoffset+boatcharlen+1+uint64(wordind)*6+6]...))
		nextoffsetval := nextoffset & 1
		haschange := false
		switch nextoffsetval {
		case 0:
			wordcuri += 1
			nextoffset = nextoffset & 0xFFFFFFFFFF
			retboatoffset, ischange := orderf.nextPushKey(uint64(nextoffset), word, wordcuri)
			if ischange == true && retboatoffset == 0 {
				panic(orderf.path + ":return error 1772.")
			}
			if retboatoffset != uint64(nextoffset>>1) {
				retboatoffset3 := uint64(wordfirstchar)<<40 | uint64(retboatoffset)<<1 | 0
				retboatoffset3bt := make([]byte, 8)
				binary.BigEndian.PutUint64(retboatoffset3bt, retboatoffset3)
				copy(boat[boatcharoffset+boatcharlen+1+uint64(wordind)*6:boatcharoffset+boatcharlen+1+uint64(wordind)*6+6], retboatoffset3bt[2:])
				haschange = true
			}
		case 1:
			var val2newid uint64
			val2newid, haschange = orderf.nextPushValue3(nextoffset, word, wordcuri)
			if haschange {
				val2newidbt := make([]byte, 8)
				binary.BigEndian.PutUint64(val2newidbt, val2newid)
				copy(boat[boatcharoffset+boatcharlen+1+uint64(wordind)*6:boatcharoffset+boatcharlen+1+uint64(wordind)*6+6], val2newidbt[2:])
			} else {
				return boatoffsetid, false
			}
		}
		if haschange == true {
			var retoffset uint64
			retoffset = orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
			return retoffset, true
		}
	} else {
		orderf.totalcount += 1
		orderf.bhavenewwrite = true
		wordlastcnt := len(word) - wordcuri
		if wordlastcnt != 0 && (boatcharlen == 0 || boatcharlen == samecnt) {
			var wordlastmemid uint64
			if wordlastcnt > 5 {
				wordfirstchar := word[wordcuri]
				if orderf.fixkeyendbt != nil {
					orderf.fixkeylen = bytes.Index(word, orderf.fixkeyendbt)
					if orderf.fixkeylen == -1 {
						orderf.fixkeylen = 0
					} else {
						orderf.fixkeylen += len(orderf.fixkeyendbt)
					}
				}
				wordlastmem, _ := orderf.buildLastWordMem(word, wordcuri, orderf.fixkeylen-wordcuri)
				var wordlastboatoffset uint64
				wordlastboatoffset = orderf.writeLastBlockNoZip(wordlastmem)
				//check4Zero(wordlastmem)
				wordlastmemid = uint64(wordfirstchar)<<40 | uint64(wordlastboatoffset)<<1 | 0
			} else if wordlastcnt > 0 {
				w3bt := make([]byte, 8)
				for wi := 0; wi < wordlastcnt; wi++ {
					w3bt[2+wi] = word[wordcuri+wi]
				}
				w3bt[7] = byte(wordlastcnt)<<1 | 1

				wordlastmemid = binary.BigEndian.Uint64(w3bt)
			}
			if wordlastmemid == 0 {
				panic(orderf.path + ":wordlastmemid is 0 error 1818.")
			}

			wordlastmemidbt := make([]byte, 8)
			binary.BigEndian.PutUint64(wordlastmemidbt, wordlastmemid)
			preisemptychild := false
			if getBoatChildLen(boat) == 0 {
				boat = BytesCombine(boat, []byte{0})
				preisemptychild = true
			}
			if mofpos != -1 {
				if mofcmpval == 1 {
					boat = BytesCombine(boat[:boatcharoffset+boatcharlen+1+uint64(mofpos)*6+6], wordlastmemidbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(mofpos)*6+6:])
				} else if mofcmpval == -1 {
					boat = BytesCombine(boat[:boatcharoffset+boatcharlen+1+uint64(mofpos)*6], wordlastmemidbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(mofpos)*6:])
				} else {
					panic(orderf.path + ":mofcmval has 0 1834.")
				}
			} else {
				boat = BytesCombine(boat, wordlastmemidbt[2:])
			}
			boat[0] |= 4
			if preisemptychild == false {
				boat[boatcharoffset+boatcharlen] += 1
			}
			//len1, off1 := boatCharLen(boat)
			var boatoffsetid2 uint64
			boatoffsetid2 = orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
			if boatoffsetid2 != boatoffsetid {
				return boatoffsetid2, true
			}
			//check4Zero(boat)
		} else {
			var boatre []byte
			if samecnt > 0 {
				boatre, _ = orderf.buildLastWordMem(boat[boatcharoffset:boatcharoffset+samecnt], -1, 0)
				boatre[0] = boatre[0] &^ 1
			} else {
				boatre = make([]byte, 1)
			}
			boatnewcnt := boatcharlen - samecnt
			var boatnextid uint64
			var boatnextfirstchar byte
			var boatnext []byte
			if boatcharlen-samecnt > 0 && (boatnewcnt > 5 || getBoatChildLen(boat) > 0) {
				boatnextcharlen := boatcharlen - samecnt
				if boatnextcharlen == 0 {
					panic(orderf.path + ":next char length is zero 1870.")
				}
				boatnextcharoffset := wordLenCalcCharOffset(int(boatnextcharlen - 1))
				boatnext = make([]byte, uint64(boatnextcharoffset)+uint64(boatnextcharlen-1)+getBoatChildLen(boat))
				copy(boatnext[boatnextcharoffset:], boat[boatcharoffset+samecnt+1:])
				boatnextfirstchar = boat[boatcharoffset+samecnt]
				boatSetCharLen(boatnext, int(boatnextcharlen-1), int(boat[0])&5)
				//fmt.Println(orderf.path,"boatnext", len(boatnext), boatnext)
				//fmt.Println(orderf.path,"boatin", boat)
			} else if boatnewcnt > 0 {
				w3bt := make([]byte, 8)
				for wi := 0; wi < int(boatnewcnt); wi++ {
					w3bt[2+wi] = boat[boatcharoffset+samecnt+uint64(wi)]
				}
				w3bt[7] = byte(boatnewcnt)<<1 | 1

				boatnextid = binary.BigEndian.Uint64(w3bt)
				boatnextfirstchar = w3bt[2]
			}
			var wordboatnextid uint64
			wordnewcnt := len(word) - wordcuri
			var wordboatnextfirstchar byte
			if wordnewcnt > 0 {
				if wordnewcnt > 5 {
					wordboatnextfirstchar = word[wordcuri]
					if orderf.fixkeyendbt != nil {
						orderf.fixkeylen = bytes.Index(word, orderf.fixkeyendbt)
						if orderf.fixkeylen == -1 {
							orderf.fixkeylen = 0
						} else {
							orderf.fixkeylen += len(orderf.fixkeyendbt)
						}
					}
					wordboatnext, _ := orderf.buildLastWordMem(word, wordcuri, orderf.fixkeylen-wordcuri)
					wordboatnextid = uint64(orderf.writeLastBlockNoZip(wordboatnext))
					wordboatnextid = uint64(wordboatnextfirstchar)<<40 | uint64(wordboatnextid)<<1 | 0
					//fmt.Println(orderf.path,"wordboatnext", len(wordboatnext), wordboatnext)
				} else {
					w3bt := make([]byte, 8)
					for wi := 0; wi < wordnewcnt; wi++ {
						w3bt[2+wi] = word[wordcuri+wi]
					}
					w3bt[7] = byte(wordnewcnt)<<1 | 1
					wordboatnextfirstchar = w3bt[2]
					wordboatnextid = binary.BigEndian.Uint64(w3bt)
				}
			}
			if boat[0]&1 == 1 {
				if boatnext != nil {
					boatnext[0] |= 1
				}
			}
			if wordboatnextid == 0 && wordcuri == len(word) {
				boatre[0] |= 1
			}

			if boatnext != nil {
				boatnextid = uint64(orderf.writeLastBlockNoZip(boatnext))
				//check4Zero(boatnext)
				boatnextid = uint64(boatnextfirstchar)<<40 | uint64(boatnextid)<<1 | 0
			}
			var bheadchanged bool
			if wordboatnextid != 0 && boatnextid != 0 {
				boatre[0] |= 4
				boatre = append(boatre, byte(1))
				if wordboatnextfirstchar > boatnextfirstchar {
					boatnextidbt := make([]byte, 8)
					binary.BigEndian.PutUint64(boatnextidbt, boatnextid)
					boatre = append(boatre, boatnextidbt[2:]...)
					wordboatnextidbt := make([]byte, 8)
					binary.BigEndian.PutUint64(wordboatnextidbt, wordboatnextid)
					boatre = append(boatre, wordboatnextidbt[2:]...)
				} else if wordboatnextfirstchar < boatnextfirstchar {
					wordboatnextidbt := make([]byte, 8)
					binary.BigEndian.PutUint64(wordboatnextidbt, wordboatnextid)
					boatre = append(boatre, wordboatnextidbt[2:]...)
					boatnextidbt := make([]byte, 8)
					binary.BigEndian.PutUint64(boatnextidbt, boatnextid)
					boatre = append(boatre, boatnextidbt[2:]...)
				}
			} else if wordboatnextid != 0 {
				boatre[0] |= 4
				boatre = append(boatre, byte(0))
				wordboatnextidbt := make([]byte, 8)
				binary.BigEndian.PutUint64(wordboatnextidbt, wordboatnextid)
				boatre = append(boatre, wordboatnextidbt[2:]...)
			} else if boatnextid != 0 {
				boatre[0] |= 4
				boatre = append(boatre, byte(0))
				boatnextidbt := make([]byte, 8)
				binary.BigEndian.PutUint64(boatnextidbt, boatnextid)
				boatre = append(boatre, boatnextidbt[2:]...)
			} else {
				if len(word) == wordcuri {
					boat[0] |= 1
					bheadchanged = true
					//fmt.Println(orderf.path,"bheadchanged:", bheadchanged, string(word[:wordcuri]), string(word), boat)
				}
			}

			if boatnewcnt != 0 || wordnewcnt != 0 {
				//fmt.Println(orderf.path,"boatre", boatre)
				var boatoffsetid2 uint64
				boatoffsetid2 = orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boatre)
				if boatoffsetid2 != boatoffsetid {
					return boatoffsetid2, true
				}
				//check4Zero(boatre)
			} else if bheadchanged {
				var boatoffsetid2 uint64
				boatoffsetid2 = orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
				if boatoffsetid2 != boatoffsetid {
					return boatoffsetid2, true
				}
			}
		}
	}

	return boatoffsetid, false
}

func (orderf *OrderFile) Push(fullkey []byte) bool {
	if !orderf.isopen {
		return false
	}
	if sysidlemem.GetSysIdleMem() < 64*1024*1024 {
		return false
	}

	orderf.markmu.Lock()
	//
	//orderf.markrmmap.Delete(string(fullkey))
	//orderf.markpushmap.Store(string(fullkey), true)

	if orderf.enablermpushlog {
		cvtbuf := make([]byte, 8)
		fullkeylen := len(fullkey)
		binary.BigEndian.PutUint32(cvtbuf, uint32(fullkeylen))
		orderf.markrmpushfile.Write(cvtbuf[:4])
		orderf.markrmpushfile.Write(fullkey)

		val := []byte{}
		vallen := len(val)
		binary.BigEndian.PutUint32(cvtbuf, uint32(vallen))
		orderf.markrmpushfile.Write(cvtbuf[:4])
		orderf.markrmpushfile.Write(val)
	}

	orderf.RealPush(fullkey)
	orderf.markmu.Unlock()
	return true
}

func (orderf *OrderFile) PushKey(key, val []byte) bool {
	if !orderf.isopen {
		return false
	}
	if sysidlemem.GetSysIdleMem() < 64*1024*1024 {
		return false
	}

	orderf.markmu.Lock()
	//orderf.markrmmap.Delete(string(key))
	//orderf.markrmpushmap.Store(string(key), val)

	if orderf.enablermpushlog {
		cvtbuf := make([]byte, 8)
		keylen := len(key)
		binary.BigEndian.PutUint32(cvtbuf, uint32(keylen))
		orderf.markrmpushfile.Write(cvtbuf[:4])
		orderf.markrmpushfile.Write(key)

		vallen := len(val)
		binary.BigEndian.PutUint32(cvtbuf, uint32(vallen))
		orderf.markrmpushfile.Write(cvtbuf[:4])
		orderf.markrmpushfile.Write(val)
	}

	orderf.RealRmPush(BytesCombine(key, orderf.fixkeyendbt), val)
	orderf.markmu.Unlock()
	return true
}

func (orderf *OrderFile) RealPush(word []byte) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, word, "RealPush 2228", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()
	if !orderf.isopen {
		return false
	}

	if len(word) >= (1<<17)-256 {
		if len(word) < 5*(1<<17) {
			var cslen int
			if orderf.compresstype == 0 {
				cslen = len(ZipEncode(nil, word, 1))
			} else if orderf.compresstype == 1 {
				cslen = len(SnappyEncode(nil, word))
			} else if orderf.compresstype == 2 {
				cslen = len(Lz4Encode(nil, word))
			} else if orderf.compresstype == 3 {
				cslen = len(XzEncode(nil, word))
			} else if orderf.compresstype == 4 {
				cslen = len(FlateEncode(nil, word, 1))
			}
			if cslen >= (1 << 17) {
				return false
			}
		} else {
			return false
		}
	}

	if orderf.lastblockendpos > ((1<<36)-32*1024*1024) || orderf.lastblockorderend > ((1<<35)-64*1024*1024) {
		orderfiletool.WriteFile(orderf.path+".reachedmax!", []byte{})
		return false
	}

	//fmt.Println(orderf.path, "filev2buf pushkey:", word)
	orderf.ordermu.Lock()
	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboattype := rootboatoffset & 1
	switch rootboattype {
	case 0, 1:
		var boatbt []byte
		boatbt = orderf.readFullBoatNoZip(rootboatoffset >> 1)
		//fmt.Println(orderf.path,"rootboatbt", rootboatoffset>>2, boatbt)
		boatbtstartlen := len(boatbt)
		boatbtcharlen, boatbtcharoffset := boatCharLen(boatbt)
		bfound := false
		wordcuri := 0
		if boatbtcharlen == 0 {
			var foundind int = -1
			if getBoatChildLen(boatbt) > 0 {
				foundind, _, _ = orderf.quickFindBuf(boatbt[boatbtcharoffset+boatbtcharlen+1:], word, wordcuri)
			}
			if foundind != -1 {
				curboatfirstchar := word[0]
				wordcuri += 1
				nextoffset := binary.BigEndian.Uint64(append(make([]byte, 2), boatbt[boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6:boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6+6]...))
				boattype := nextoffset & 1
				switch boattype {
				case 0:
					if byte(nextoffset>>40) != word[0] {
						//fmt.Println(byte(nextoffset>>40), word[0], word)
						panic(orderf.path + ":first word not equal 2075.")
					}
					nextoffset = nextoffset & 0xFFFFFFFFFF
					retnextoffset, _ := orderf.nextPushKey(uint64(nextoffset), word, wordcuri)
					if retnextoffset != uint64(nextoffset>>1) {
						if retnextoffset == 0 {
							panic(orderf.path + ":return error 2083.")
						}
						retnextoffset3 := uint64(curboatfirstchar)<<40 | uint64(retnextoffset)<<1 | 0
						retnextoffset3bt := make([]byte, 8)
						binary.BigEndian.PutUint64(retnextoffset3bt, retnextoffset3)
						copy(boatbt[boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6:boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6+6], retnextoffset3bt[2:])
						var retrootoffset uint64
						retrootoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
						if retrootoffset != uint64(rootboatoffset>>1) {
							tempbt := make([]byte, 8)
							binary.BigEndian.PutUint64(tempbt, retrootoffset<<1|0)
							copy(orderf.shorttable[0:6], tempbt[2:])
						}
					}
				default:
					panic(orderf.path + ":root boat have wrong boat type 2097.")
				}
				bfound = true
			}
		}

		if bfound == false {
			if len(word) > 0 {
				boatbtcharlen, boatbtcharoffset := boatCharLen(boatbt)
				var foundpos, mofpos, mofcmpval int = -1, -1, -1
				if getBoatChildLen(boatbt) > 0 {
					//fmt.Println(orderf.path,"boatbt", boatbt)
					foundpos, mofpos, mofcmpval = orderf.quickFindBuf(boatbt[boatbtcharlen+boatbtcharoffset+1:], word, 0)
				}
				if orderf.fixkeyendbt != nil {
					orderf.fixkeylen = bytes.Index(word, orderf.fixkeyendbt)
					if orderf.fixkeylen == -1 {
						orderf.fixkeylen = 0
					} else {
						orderf.fixkeylen += len(orderf.fixkeyendbt)
					}
				}
				wordlastmem, _ := orderf.buildLastWordMem(word, 0, orderf.fixkeylen-0)
				var wordlastmemid uint64
				wordlastmemid = uint64(orderf.writeLastBlockNoZip(wordlastmem))
				//check4Zero(wordlastmem)
				wordlastmemidbt := make([]byte, 8)
				binary.BigEndian.PutUint64(wordlastmemidbt, uint64(word[0])<<40|wordlastmemid<<1|0)
				if foundpos != -1 {
					//fmt.Println(orderf.path,"boatbt", boatbt, word)
					panic(orderf.path + ":root boat insert found same first char boat 2120.")
				}
				if mofpos == -1 {
					boatbt = BytesCombine(boatbt, []byte{0}, wordlastmemidbt[2:])
				} else if mofcmpval > 0 {
					boatbt = BytesCombine(boatbt[:boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6+6], wordlastmemidbt[2:], boatbt[boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6+6:])
					boatbt[boatbtcharlen+boatbtcharoffset] += 1
				} else if mofcmpval < 0 {
					boatbt = BytesCombine(boatbt[:boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6], wordlastmemidbt[2:], boatbt[boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6:])
					boatbt[boatbtcharlen+boatbtcharoffset] += 1
				}
				boatbt[0] |= 4
				//fmt.Println(orderf.path,"boatbt634", boatbt)
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 0
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
				orderf.totalcount += 1
				orderf.bhavenewwrite = true
			} else {
				if boatbt[0]&1 == 0 {
					orderf.totalcount += 1
					orderf.bhavenewwrite = true
				}
				boatbt[0] |= 1
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 1
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
			}
		}
	}
	orderf.ordermu.Unlock()
	return true
}

func (orderf *OrderFile) nextRmKey(boatoffsetid uint64, boat, sentence []byte, sentencecuri int, broot bool) (needdeloffset, delok bool) {
	boatcharlen, boatcharoffset := boatCharLen(boat)
	boatstartlen := len(boat)

	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}

	if samecnt == boatcharlen || samecnt > 0 || sentencecuri == len(sentence) {
		if boat[0]&1 == 1 {
			if sentencecuri == len(sentence) {
				if getBoatChildLen(boat) == 0 {
					orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), []byte{})
					orderf.totalcount -= 1
					orderf.bhavenewwrite = true
					return true, true
				} else if samecnt == boatcharlen {
					boat[0] = boat[0] & 0xFE
					orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
					orderf.totalcount -= 1
					orderf.bhavenewwrite = true
					return false, true
				}
			}
		}

		var foundind int = -1
		if getBoatChildLen(boat) > 0 && samecnt == boatcharlen && sentencecuri < len(sentence) {
			foundind, _, _ = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
		} else if getBoatChildLen(boat) == 1+6 && sentencecuri == len(sentence) {
			foundind = 0
		}
		if foundind != -1 {
			//fmt.Println(orderf.path, "foundind:", foundind, "sentence, sentencecuri:", sentence, sentencecuri, boat)
			nextoffset2 := binary.BigEndian.Uint64(append(make([]byte, 2), boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6]...))
			wordtypeval2 := nextoffset2 & 1
			switch wordtypeval2 {
			case 0:
				if sentencecuri != len(sentence) {
					sentencecuri += 1
				}
				nextoffset2 = nextoffset2 & 0xFFFFFFFFFF
				nextoffset2id := nextoffset2 >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				needdeloffset2, delresult := orderf.nextRmKey(uint64(nextoffset2id), subboat, sentence, sentencecuri, false)
				if needdeloffset2 == true {
					boat = BytesCombine(boat[:boatcharoffset+boatcharlen+1+uint64(foundind)*6], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6+6:])
					if boat[boatcharlen+boatcharoffset] >= 1 {
						boat[boatcharlen+boatcharoffset] -= 1
					} else {
						boat[0] = boat[0] &^ (4)
						boat = boat[:boatcharlen+boatcharoffset]
					}
					if getBoatChildLen(boat) == 0 && (boat[0]&1) == 0 {
						if broot == false {
							orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), []byte{})
						}
						if delresult == false {
							orderf.totalcount -= 1
							orderf.bhavenewwrite = true
						}
						return true, true
					} else {
						orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
						if delresult == false {
							orderf.totalcount -= 1
							orderf.bhavenewwrite = true
						}
						return false, true
					}
				}
				if delresult == true {
					return false, true
				}
			case 1:
				var bsentencefullma bool
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				if sentencecuri == len(sentence) {
					bsentencefullma = true
				} else {
					remainlen := len(sentence) - sentencecuri
					if remainlen <= int(v3btnlen) {
						if bytes.Compare(sentence[sentencecuri:], v3btn[2 : 2+remainlen][:len(sentence[sentencecuri:])]) == 0 {
							bsentencefullma = true
						}
					}
				}
				if bsentencefullma {
					boat = BytesCombine(boat[:boatcharoffset+boatcharlen+1+uint64(foundind)*6], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6+6:])
					if boat[boatcharlen+boatcharoffset] >= 1 {
						boat[boatcharlen+boatcharoffset] -= 1
					} else {
						boat[0] = boat[0] &^ (4)
						boat = boat[:boatcharlen+boatcharoffset]
					}
					if getBoatChildLen(boat) == 0 && (boat[0]&1) == 0 {
						if broot == false {
							orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), []byte{})
						}
						orderf.totalcount -= 1
						orderf.bhavenewwrite = true
						return true, true
					} else {
						orderf.writeOldBoatNoZip(boatoffsetid, uint64(boatstartlen), boat)
						orderf.totalcount -= 1
						orderf.bhavenewwrite = true
						return false, true
					}
				} else {
					return false, false
				}
			}
		}
	}
	return false, false
}

func (orderf *OrderFile) RmKey(key []byte) bool {
	if !orderf.isopen {
		return false
	}
	orderf.markmu.Lock()
	//orderf.markrmmap.Store(string(key), true)
	//orderf.markrmpushmap.Delete(string(key))
	//orderf.markpushmap.Delete(string(key))

	if orderf.enablermpushlog {
		cvtbuf := make([]byte, 8)
		keylen := len(key)
		binary.BigEndian.PutUint32(cvtbuf, uint32(keylen)|(uint32(1)<<31))
		orderf.markrmpushfile.Write(cvtbuf[:4])
		orderf.markrmpushfile.Write(key)
	}

	orderf.RealRm(BytesCombine(key, orderf.fixkeyendbt))
	orderf.markmu.Unlock()
	return true
}

func (orderf *OrderFile) RealRm(key []byte) bool {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, key, "RealRm 2475", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()
	if !orderf.isopen {
		return false
	}

	//fmt.Println(orderf.path, "filev2buf RmKey:", key)
	orderf.ordermu.Lock()
	//orderf.ErrorRecord("R", key, nil)

	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	rootboattype := rootboatoffset & 1
	delok := false
	if rootboattype == 0 {
		if len(key) > 0 {
			needdel, delok1 := orderf.nextRmKey(rootboatoffset>>1, rootboat, key, 0, true)
			//fmt.Println(orderf.path,"needdel, delok1:", needdel, delok1)
			delok = delok1
			if needdel {
				boatcharlen, boatcharoffset := boatCharLen(rootboat)
				boatstartlen := len(rootboat)
				foundind, _, _ := orderf.quickFindBuf(rootboat[boatcharoffset+boatcharlen+1:], key, 0)
				rootboat = BytesCombine(rootboat[:boatcharoffset+boatcharlen+1+uint64(foundind)*6], rootboat[boatcharoffset+boatcharlen+1+uint64(foundind)*6+6:])
				if rootboat[boatcharlen+boatcharoffset] >= 1 {
					rootboat[boatcharlen+boatcharoffset] -= 1
				} else {
					rootboat[0] = rootboat[0] &^ (4)
					rootboat = rootboat[:boatcharlen+boatcharoffset]
				}
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatstartlen), rootboat)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 0
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
			}
		} else if rootboat[0]&1 != 0 {
			rootboat[0] = rootboat[0] &^ 1
			var retrootboatoffset uint64
			retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(len(rootboat)), rootboat)
			if retrootboatoffset != rootboatoffset>>1 {
				retrootboatoffset = retrootboatoffset<<1 | 0
				tempbt := make([]byte, 8)
				binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
				copy(orderf.shorttable[0:6], tempbt[2:])
			}
			orderf.totalcount -= 1
			orderf.bhavenewwrite = true
		}
	}
	orderf.ordermu.Unlock()
	return delok
}

func (orderf *OrderFile) quickFindBuf(boatchild, word []byte, wordcuri int) (foundind, mofind, mofcmpval int) {
	if len(word) == wordcuri {
		return -1, -1, -1
	}
	cnt := len(boatchild) / 6
	if cnt == 0 {
		return -1, -1, -1
	}
	start := 0
	end := len(boatchild)/6 - 1
	cur := (start + end) / 2
	findbuf := make([]byte, 8)
	for true {
		copy(findbuf[2:], boatchild[cur*6:cur*6+6])
		subboatoffset := binary.BigEndian.Uint64(findbuf)
		subboatfirstchar := byte(subboatoffset >> 40)
		subboattype := subboatoffset & 1
		switch subboattype {
		case 0:
			if word[wordcuri] < subboatfirstchar {
				end = cur
				cur2 := int(float64(start+end) / 2)
				if cur2 == cur {
					return -1, cur, -1
				} else {
					cur = cur2
				}
			} else if word[wordcuri] > subboatfirstchar {
				start = cur
				cur2 := int(float64(start+end) / 2)
				if cur2 == cur {
					if end-start == 1 {
						start = end
						cur = end
					} else {
						return -1, cur, 1
					}
				} else {
					cur = cur2
				}
			} else {
				return cur, -1, -1
			}
		case 1:
			binary.BigEndian.PutUint64(findbuf, subboatoffset)
			if word[wordcuri] < findbuf[2] {
				end = cur
				cur2 := int(float64(start+end) / 2)
				if cur2 == cur {
					return -1, cur, -1
				} else {
					cur = cur2
				}
			} else if word[wordcuri] > findbuf[2] {
				start = cur
				cur2 := int(float64(start+end) / 2)
				if cur2 == cur {
					if end-start == 1 {
						start = end
						cur = end
					} else {
						return -1, cur, 1
					}
				} else {
					cur = cur2
				}
			} else {
				return cur, -1, -1
			}
		default:
			panic(orderf.path + ":boat type has 1 or 0 error 2356.")
		}
	}
	return -1, -1, -1
}

//压缩byte,Compress Level, -1   (Default) 0x78,0x9C Compress Level, 0 0x78,0x1 Compress Level, 1 0x78,0x1 Compress Level, 2 0x78,0x5E Compress Level, 3 0x78,0x5E Compress Level, 4 0x78,0x5E Compress Level, 5 0x78,0x5E Compress Level, 6 0x78,0x9C Compress Level, 7 0x78,0xDA Compress Level, 8 0x78,0xDA Compress Level, 9 (Max compressLevel)0x78,0xDA
func ZipEncode(outbuf, input []byte, level int) []byte {
	var outbt []byte
	if outbuf != nil {
		outbt = outbuf
	}
	buf := bytes.NewBuffer(outbt)
	buf.Reset()
	compressor, err := zlib.NewWriterLevel(buf, level)
	if err != nil {
		panic("compress error 2362.")
	}
	compressor.Write(input)
	compressor.Close()
	if outbuf != nil {
		return outbt[:buf.Len()]
	} else {
		return buf.Bytes()
	}
}

//解压byte
func ZipDecode(outbuf, input []byte) []byte {
	if len(input) == 0 {
		return []byte{}
	}
	b := bytes.NewReader(input)
	r, err := zlib.NewReader(b)
	if r == nil {
		return make([]byte, 0)
	}
	defer r.Close()
	if err != nil {
		panic("unZipByte error 2394.")
	}
	b2 := bytes.NewBuffer(outbuf)
	b2.Reset()
	outw := bufio.NewWriter(b2)
	if _, err := io.Copy(outw, r); err != nil {
		fmt.Println(orderfiletool.GetGoRoutineID(), " ZipDecode src error. src:", input)
		panic("DelateDecode Error 2478.")
	}
	if outbuf != nil {
		return outbuf[:b2.Len()]
	} else {
		return b2.Bytes()
	}
}

func SnappyEncode(outbuf, input []byte) []byte {
	return snappy.Encode(outbuf, input)
}

//解压byte
func SnappyDecode(outbuf, input []byte) []byte {
	un, _ := snappy.Decode(outbuf, input)
	return un
}

func Lz4Encode(buf, src []byte) []byte {
	b := bytes.NewBuffer(buf)
	b.Reset()
	zw := lz4.NewWriter(b)
	zw.Write(src)
	zw.Close()
	if buf != nil {
		return buf[:b.Len()]
	} else {
		return b.Bytes()
	}
}

func Lz4Decode(outbuf, src []byte) []byte {
	wr := bytes.NewBuffer(outbuf)
	wr.Reset()
	var b *bytes.Reader
	b = bytes.NewReader(src)
	zr := lz4.NewReader(b)
	uncompressedSize, err := io.Copy(wr, zr)
	if err != nil {
		fmt.Println("Lz4Decode src", src)
		panic("Lz4Lz4Decode error")
	}
	if outbuf != nil {
		return outbuf[:uncompressedSize]
	} else {
		return wr.Bytes()
	}
}

func XzEncode(buf, src []byte) []byte {
	b := bytes.NewBuffer(buf)
	b.Reset()
	w, err := xz.NewWriter(b)
	if err != nil {
		log.Fatalf("xz.NewWriter error %s", err)
	}
	if _, err := io.WriteString(w, string(src)); err != nil {
		log.Fatalf("WriteString error %s", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("w.Close error %s", err)
	}
	if buf != nil {
		return buf[:b.Len()]
	} else {
		return b.Bytes()
	}
}

func XzDecode(outbuf, src []byte) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	wr := bytes.NewBuffer(outbuf)
	wr.Reset()
	b := bytes.NewBuffer(src)
	xzr, xzre := xz.NewReader(b)
	if xzre != nil {
		log.Fatalf("NewReader error %s", xzre)
	}
	uncompressedSize, err := io.Copy(wr, xzr)
	if err != nil {
		log.Fatalf("io.Copy error %s", err)
	}
	if outbuf != nil {
		return outbuf[:uncompressedSize]
	} else {
		return wr.Bytes()
	}
}

func FlateEncode(buf, src []byte, level int) []byte {
	b := bytes.NewBuffer(buf)
	b.Reset()
	zw, err := flate.NewWriter(b, 1)
	if err != nil {
		panic("FlateEncode Error 2460.")
	}
	zw.Write(src)
	zw.Close()
	if buf != nil {
		return buf[:b.Len()]
	} else {
		return b.Bytes()
	}
}

func FlateDecode(outbuf, src []byte) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	b := bytes.NewBuffer(src)
	zr := flate.NewReader(nil)
	if err := zr.(flate.Resetter).Reset(b, nil); err != nil {
		fmt.Println(" FlateDecode src error. src:", src)
		panic("FlateDecode error 2471.")
	}
	b2 := bytes.NewBuffer(outbuf)
	b2.Reset()
	outw := bufio.NewWriter(b2)
	if _, err := io.Copy(outw, zr); err != nil {
		fmt.Println(" FlateDecode src error. src:", src)
		panic("DelateDecode Error 2478.")
	}
	if err := zr.Close(); err != nil {
		fmt.Println(" FlateDecode src error. src:", src)
		panic("FlateDecode error 2481.")
	}
	if outbuf != nil {
		return outbuf[:b2.Len()]
	} else {
		return b2.Bytes()
	}
}

func boatCharLen(boat []byte) (charlen, chardelta uint64) {
	charlen = uint64(boat[0] >> 3)
	chardelta = 1
	if (boat[0] & 2) != 0 {
		charlen = (charlen << 7) | uint64(boat[1]>>1)
		chardelta += 1
		if (boat[1] & 1) != 0 {
			charlen = (charlen << 7) | uint64(boat[2]>>1)
			chardelta += 1
			if (boat[2] & 1) != 0 {
				charlen = (charlen << 7) | uint64(boat[3]>>1)
				chardelta += 1
			}
		}
	}
	return charlen, chardelta
}

func getBoatChildLen(boat []byte) uint64 {
	var childlen uint64
	if (boat[0] & 4) != 0 {
		boatcharlen, boatcharoffset := boatCharLen(boat)
		childlen = 1 + (uint64(boat[boatcharoffset+boatcharlen])+1)*6
	}
	return childlen
}

//exclude first char
func (orderf *OrderFile) buildLastWordMem(word []byte, wordcuri, infixbytes int) (boat []byte, boatcharoffset int) {
	if infixbytes < 0 || wordcuri == -1 {
		infixbytes = 0
	}
	wordcuri2 := wordcuri + 1
	if infixbytes > 0 {
		wlen2 := len(word) - (wordcuri + infixbytes)
		if infixbytes > 1 && wlen2 > 8 {
			wordcuri2 = wordcuri + infixbytes
		} else {
			infixbytes = 0
		}
	}
	boatcharoffset = 1
	boatlen := 0
	lastwordlen := len(word) - wordcuri2
	if lastwordlen < (1 << 5) {
		boatlen = 1 + lastwordlen
	} else if lastwordlen < (1 << 12) {
		boatlen = 2 + lastwordlen
		boatcharoffset = 2
	} else if lastwordlen < (1 << 19) {
		boatlen = 3 + lastwordlen
		boatcharoffset = 3
	} else if lastwordlen < (1 << 26) {
		boatlen = 4 + lastwordlen
		boatcharoffset = 4
	}
	boat = make([]byte, boatlen)
	lastwordlencp := lastwordlen
	if lastwordlencp < (1 << 5) {
		boat[0] = byte(lastwordlencp<<3 | 1)
	} else if lastwordlencp < (1 << 12) {
		boat[1] = byte((lastwordlencp & 0x7f) << 1)
		lastwordlencp = lastwordlencp >> 7
		boat[0] = byte((lastwordlencp&0x1f)<<3 | 3)
	} else if lastwordlencp < (1 << 19) {
		boat[2] = byte((lastwordlencp & 0x7f) << 1)
		lastwordlencp = lastwordlencp >> 7
		boat[1] = byte((lastwordlencp&0x7f)<<1 | 1)
		lastwordlencp = lastwordlencp >> 7
		boat[0] = byte((lastwordlencp&0x1f)<<3 | 3)
	} else if lastwordlencp < (1 << 26) {
		boat[3] = byte((lastwordlencp & 0x7f) << 1)
		lastwordlencp = lastwordlencp >> 7
		boat[2] = byte((lastwordlencp&0x7f)<<1 | 1)
		lastwordlencp = lastwordlencp >> 7
		boat[1] = byte((lastwordlencp&0x7f)<<1 | 1)
		lastwordlencp = lastwordlencp >> 7
		boat[0] = byte((lastwordlencp&0x1f)<<3 | 3)
	}
	copy(boat[boatcharoffset:boatcharoffset+lastwordlen], word[wordcuri2:])
	if infixbytes == 0 {
		return boat, boatcharoffset
	} else {
		charoffset := wordLenCalcCharOffset(infixbytes - 2)
		paboat := make([]byte, charoffset+(infixbytes-2)+1+6)
		copy(paboat[charoffset:], word[wordcuri+1:wordcuri+infixbytes-1])
		boatSetCharLen(paboat, int(infixbytes-2), 4)
		boatoffset := orderf.writeLastBlockNoZip(boat)
		wordlastmemid := uint64(word[wordcuri+infixbytes-1])<<40 | uint64(boatoffset)<<1 | 0
		pachildbt := make([]byte, 8)
		binary.BigEndian.PutUint64(pachildbt, wordlastmemid)
		copy(paboat[len(paboat)-6:], pachildbt[2:])
		return paboat, charoffset
	}
}

func wordLenCalcCharOffset(wordlen int) int {
	if wordlen < (1 << 5) {
		return 1
	} else if wordlen < (1 << 12) {
		return 2
	} else if wordlen < (1 << 19) {
		return 3
	} else if wordlen < (1 << 26) {
		return 4
	} else {
		return -1
	}
}

func boatSetCharLen(boat []byte, carlen, hasvalchild int) {
	if carlen < (1 << 5) {
		boat[0] = byte(carlen<<3 | hasvalchild)
	} else if carlen < (1 << 12) {
		boat[1] = byte((carlen & 0x7f) << 1)
		carlen = carlen >> 7
		boat[0] = byte((carlen&0x1f)<<3 | 2 | hasvalchild)
	} else if carlen < (1 << 19) {
		boat[2] = byte((carlen & 0x7f) << 1)
		carlen = carlen >> 7
		boat[1] = byte((carlen&0x7f)<<1 | 1)
		carlen = carlen >> 7
		boat[0] = byte((carlen&0x1f)<<3 | 2 | hasvalchild)
	} else if carlen < (1 << 26) {
		boat[3] = byte((carlen & 0x7f) << 1)
		carlen = carlen >> 7
		boat[2] = byte((carlen&0x7f)<<1 | 1)
		carlen = carlen >> 7
		boat[1] = byte((carlen&0x7f)<<1 | 1)
		carlen = carlen >> 7
		boat[0] = byte((carlen&0x1f)<<3 | 2 | hasvalchild)
	}
}

func boatHasNext(partboat []byte) (lencmpval int, charlen, charoffset, childlen uint64, bsuccess bool) {
	if len(partboat) == 0 {
		//fmt.Println("boathasNext error 3698", partboat)
		return -1, 0, 0, 0, false
	}
	charlen = uint64(partboat[0] >> 3)
	charoffset = 1
	if (partboat[0] & 2) != 0 {
		if len(partboat) <= 1 {
			//fmt.Println("boathasNext error 3705", partboat)
			return -1, 0, 0, 0, false
		}
		charlen = (charlen << 7) | uint64(partboat[1]>>1)
		charoffset += 1
		if (partboat[1] & 1) != 0 {
			if len(partboat) <= 2 {
				//fmt.Println("boathasNext error 3712", partboat)
				return -1, 0, 0, 0, false
			}
			charlen = (charlen << 7) | uint64(partboat[2]>>1)
			charoffset += 1
			if (partboat[2] & 1) != 0 {
				if len(partboat) <= 3 {
					//fmt.Println("boathasNext error 3719", partboat)
					return -1, 0, 0, 0, false
				}
				charlen = (charlen << 7) | uint64(partboat[3]>>1)
				charoffset += 1
			}
		}
	}

	if uint64(len(partboat)) < charlen+charoffset {
		//fmt.Println("boathasNext error 3729", charlen, charoffset, partboat)
		return -1, 0, 0, 0, false
	}
	if partboat[0]&4 != 0 {
		if uint64(len(partboat)) < charlen+charoffset+1 {
			//fmt.Println("boathasNext error 3734", charlen, charoffset, partboat)
			return -1, 0, 0, 0, false
		}
		childlen = 1 + (uint64(partboat[charlen+charoffset])+1)*6
		if uint64(len(partboat)) < charlen+charoffset+childlen {
			//fmt.Println("boathasNext error 3739", charlen, charoffset, childlen, partboat)
			return -1, 0, 0, 0, false
		}
	}

	if uint64(len(partboat)) == charlen+charoffset+childlen {
		return 0, charlen, charoffset, childlen, true
	}
	return 1, charlen, charoffset, childlen, true
}

func getBlockPos(blockbt []byte) (orderstart, orderlen, datapos, dataposlen, truncblock uint64) {
	part1 := binary.BigEndian.Uint64(blockbt[0:8])
	part2 := binary.BigEndian.Uint16(blockbt[8:10])
	orderstart = part1 >> 28
	datapos = ((part1 & 0xFFFFFFF) << 7) | uint64(blockbt[10]&0x7f)
	dataposlen = (uint64(part2) << 1) | uint64(blockbt[10]>>7)
	orderlen = (binary.BigEndian.Uint64(blockbt[11:11+8]) >> 28) - orderstart
	return orderstart, orderlen, datapos, dataposlen, truncblock
}

func setBlockPos(blockbt []byte, orderstart, orderlen, datapos, dataposlen, orderlenchange uint64) {
	binary.BigEndian.PutUint64(blockbt[0:8], (orderstart<<28)|(datapos>>7))
	binary.BigEndian.PutUint16(blockbt[8:10], uint16(dataposlen>>1))
	blockbt[10] = byte(((dataposlen & 1) << 7) | (datapos & 0x7F))
	if orderlenchange == 1 {
		binary.BigEndian.PutUint64(blockbt[11:11+8], (orderstart+orderlen)<<28)
	}
}

func (orderf *OrderFile) nextNextKey(boat, sentence []byte, sentencecuri int) (foundkey []byte, bfound, bmovetonext bool) {
	boatcharlen, boatcharoffset := boatCharLen(boat)

	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}
	//fmt.Println(orderf.path,"nextNextKey:", samecnt, boatcharlen, samecnt, sentencecuri, sentence, boat)
	if boat[0]&1 == 1 {
		var cmplen uint64
		if uint64(sentencecuri)+(boatcharlen-samecnt) < uint64(len(sentence)) {
			cmplen = boatcharlen - samecnt
		} else {
			cmplen = uint64(len(sentence) - sentencecuri)
		}
		if sentencecuri == len(sentence) && samecnt != boatcharlen || samecnt != boatcharlen && bytes.Compare(sentence[sentencecuri:sentencecuri+int(cmplen)], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+cmplen]) == -1 {
			foundkey := BytesCombine(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+(boatcharlen-samecnt)])
			if bytes.Compare(foundkey, sentence) > 0 {
				//fmt.Println(orderf.path,"4 return")
				return foundkey, true, false
			} else {
				if getBoatChildLen(boat) != 0 {
					makeoffsetbt := make([]byte, 8)
					copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(0)*6:boatcharoffset+boatcharlen+1+uint64(0)*6+6])
					nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
					wordtypeval2 := nextoffset2 & 1
					nearchar := []byte{}
					nearchar = append(nearchar, sentence[0:sentencecuri]...)
					for true {
						nearchar = append(nearchar, byte(nextoffset2>>40))
						nextoffset3 := nextoffset2 & 0xFFFFFFFFFF
						wordtypeval2 = nextoffset3 & 1
						nextoffset2id := nextoffset3 >> 1
						switch wordtypeval2 {
						case 0:
							//fmt.Println(orderf.path,"nextoffset2id:", nextoffset2id)
							subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
							//fmt.Println(orderf.path,"nextoffset2id2:", nextoffset2id, subboat, subboat[0]&1)
							boatcharlen, boatcharoffset = boatCharLen(subboat)
							//fmt.Println(orderf.path,"boatcharlen, boatcharoffset:", boatcharlen, boatcharoffset)
							nearchar = append(nearchar, subboat[boatcharoffset:boatcharoffset+boatcharlen]...)
							if subboat[0]&1 == 1 {
								//fmt.Println(orderf.path,"2415 return", subboat)
								return nearchar, true, false
							} else {
								if getBoatChildLen(subboat) > 0 {
									makeoffsetbt := make([]byte, 8)
									copy(makeoffsetbt[2:], subboat[boatcharoffset+boatcharlen+1+uint64(0)*6:boatcharoffset+boatcharlen+1+uint64(0)*6+6])
									nextoffset2 = binary.BigEndian.Uint64(makeoffsetbt)
								} else {
									//fmt.Println(orderf.path,"2423 return")
									return []byte{}, false, false
								}
							}
						case 1:
							v3btn := make([]byte, 8)
							binary.BigEndian.PutUint64(v3btn, nextoffset2)
							v3btnlen := v3btn[7] >> 1
							//fmt.Println(orderf.path,"v3btn:", v3btn, v3btnlen)
							nearchar = append(nearchar, v3btn[2+1:2+1+v3btnlen-1]...)
							return nearchar, true, false
						}
					}
				}
				//fmt.Println(orderf.path,"56 return", "samecnt:", samecnt, "foundkey:", string(foundkey), string(sentence[:sentencecuri]), string(sentence), boat)
				return []byte{}, false, true
			}
		}
	}

	var foundind, mofind, mofcmpval int = -1, -1, -1
	if getBoatChildLen(boat) > 0 && samecnt == boatcharlen && sentencecuri < len(sentence) {
		foundind, mofind, mofcmpval = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
	} else if getBoatChildLen(boat) == 1+6 && sentencecuri == len(sentence) {
		foundind = 0
	}
	//fmt.Println(orderf.path,"2342 foundind:", foundind)
	if foundind == -1 && mofind != -1 {
		if mofcmpval < 0 {
			foundind = mofind
			//fmt.Println(orderf.path,"2346 foundind = mofind + 1", foundind, mofind, len(sentence), sentencecuri, sentence)
		} else {
			if mofind-1 >= 0 {
				foundind = mofind - 1
			} else {
				foundind = mofind
			}
			//fmt.Println(orderf.path,"2350 foundind = mofind", foundind)
		}
	}
	if foundind == -1 && sentencecuri == len(sentence) && getBoatChildLen(boat) != 0 {
		sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
		sentencecuri += int(boatcharlen - samecnt)
		samecnt = boatcharlen
		foundind = 0
	}
	if foundind == -1 && samecnt != boatcharlen && getBoatChildLen(boat) != 0 {
		var cmplen uint64
		if uint64(sentencecuri)+(boatcharlen-samecnt) < uint64(len(sentence)) {
			cmplen = boatcharlen - samecnt
		} else {
			cmplen = uint64(len(sentence) - sentencecuri)
		}
		if bytes.Compare(sentence[sentencecuri:sentencecuri+int(cmplen)], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+cmplen]) == -1 {
			sentence = append(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
			sentencecuri += int(boatcharlen - samecnt)
			samecnt = boatcharlen
			foundind = 0
		}
	}
	//fmt.Println(orderf.path,"nextNextKey foundind", foundind, "childcnt:", int(getBoatChildLen(boat))/6, "samecnt:", samecnt, "sentencecuri", sentencecuri, string(sentence[:sentencecuri]), "sentence:", string(sentence), boat)
	for int(foundind) >= 0 && int(foundind) < int(getBoatChildLen(boat))/6 {
		makeoffsetbt := make([]byte, 8)
		//fmt.Println(orderf.path,"foundind:", foundind, "childcnt:", int(getBoatChildLen(boat))/6, boatcharoffset, boatcharlen, boat)
		copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
		nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
		//fmt.Println(orderf.path,"firstchar compare:", string([]byte{byte(nextoffset2 >> 40)}), string(sentence[sentencecuri:sentencecuri+1]))
		wordtypeval2 := nextoffset2 & 1
		if sentencecuri < len(sentence) && byte(nextoffset2>>40) == sentence[sentencecuri] {
			switch wordtypeval2 {
			case 0:
				sentencecuri++
				nextoffset2 = nextoffset2 & 0xFFFFFFFFFF
				nextoffset2id := nextoffset2 >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				fndkey, bfndkey, bmovenext := orderf.nextNextKey(subboat, sentence, sentencecuri)
				//fmt.Println(orderf.path,"2365 fndkey, bfndkey, bmovenext", fndkey, bfndkey, bmovenext, "foundind:", foundind)
				curendkey := []byte{}
				if bmovenext && subboat[0]&1 == 1 {
					boatcharlen, boatcharoffset := boatCharLen(subboat)
					curendkey = append(sentence[:sentencecuri], subboat[boatcharoffset:boatcharoffset+boatcharlen]...)
					//fmt.Println(orderf.path,"2529 curendkey:", curendkey, boat, subboat)
				}
				curendkey2 := []byte{}
				if bmovenext && subboat[0]&1 == 1 && boat[0]&1 == 1 {
					boatcharlen2, boatcharoffset2 := boatCharLen(subboat)
					curendkey2 = BytesCombine(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+(boatcharlen-samecnt)], subboat[boatcharoffset2:boatcharoffset2+boatcharlen2])
				}
				//fmt.Println(orderf.path,"curendkey:", curendkey, "curendkey2:", curendkey2)
				if bmovenext && subboat[0]&1 == 1 && bytes.Compare(curendkey, sentence) > 0 {
					//fmt.Println(orderf.path,"2538 return")
					return curendkey, true, false
				} else if bmovenext && subboat[0]&1 == 1 && boat[0]&1 == 1 && bytes.Compare(curendkey2, sentence) > 0 {
					//fmt.Println(orderf.path,"2541 return", "subboat:", subboat, "boat:", boat)
					return curendkey2, true, false
				} else if bmovenext {
					sentencecuri -= 1
					if int(foundind)+1 < int(getBoatChildLen(boat)/6) {
						foundind = foundind + 1
						//fmt.Println(orderf.path,"new foundind", foundind, boat)
						continue
					} else {
						return []byte{}, false, true
					}
				} else {
					return fndkey, bfndkey, false
				}
			case 1:
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				foundkey := BytesCombine(sentence[:sentencecuri], v3btn[2:2+v3btnlen])
				if bytes.Compare(foundkey, sentence) > 0 {
					//fmt.Println(orderf.path,"3 return:", foundkey, true, false)
					return foundkey, true, false
				} else {
					if uint64(foundind)+1 < getBoatChildLen(boat)/6 {
						//fmt.Println(orderf.path,"3 return2")
						foundind = foundind + 1
						continue
					} else {
						//fmt.Println(orderf.path,"3 return3")
						return []byte{}, false, true
					}
				}
			}
		} else if len(sentence) == 0 || sentencecuri == len(sentence) || sentencecuri < len(sentence) && byte(nextoffset2>>40) > sentence[sentencecuri] {
			//fmt.Println(orderf.path,"77777777777777777777")
			nearchar := []byte{}
			nearchar = append(nearchar, sentence[0:sentencecuri]...)
			cnt := 0
			for true {
				cnt += 1
				nearchar = append(nearchar, byte(nextoffset2>>40))
				nextoffset3 := nextoffset2 & 0xFFFFFFFFFF
				wordtypeval2 = nextoffset3 & 1
				nextoffset2id := nextoffset3 >> 1
				switch wordtypeval2 {
				case 0:
					//fmt.Println(orderf.path,"nextoffset2id:", nextoffset2id)
					subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
					//fmt.Println(orderf.path,"nextoffset2id2:", nextoffset2id, subboat, subboat[0]&1)
					boatcharlen, boatcharoffset = boatCharLen(subboat)
					//fmt.Println(orderf.path,"boatcharlen, boatcharoffset:", boatcharlen, boatcharoffset)
					nearchar = append(nearchar, subboat[boatcharoffset:boatcharoffset+boatcharlen]...)
					if subboat[0]&1 == 1 {
						//fmt.Println(orderf.path,"2415 return", byte(nextoffset2>>40), subboat)
						return nearchar, true, false
					} else {
						if getBoatChildLen(subboat) > 0 {
							makeoffsetbt := make([]byte, 8)
							copy(makeoffsetbt[2:], subboat[boatcharoffset+boatcharlen+1+uint64(0)*6:boatcharoffset+boatcharlen+1+uint64(0)*6+6])
							nextoffset2 = binary.BigEndian.Uint64(makeoffsetbt)
						} else {
							//fmt.Println(orderf.path,"2423 return")
							return []byte{}, false, false
						}
					}
				case 1:
					v3btn := make([]byte, 8)
					binary.BigEndian.PutUint64(v3btn, nextoffset2)
					v3btnlen := v3btn[7] >> 1
					//fmt.Println(orderf.path, "v3btn:", v3btn, v3btnlen)
					nearchar = append(nearchar, v3btn[2+1:2+1+v3btnlen-1]...)
					return nearchar, true, false
				}
			}
		} else {
			if uint64(foundind)+1 < getBoatChildLen(boat)/6 {
				foundind = foundind + 1
				continue
			} else {
				//fmt.Println(orderf.path,"not found1")
				return []byte{}, false, true
			}
		}
	}
	//fmt.Println(orderf.path,"not found2")
	return []byte{}, false, true
}

func (orderf *OrderFile) NextKey(curkey []byte) (nextkey []byte, bnext bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, curkey, "NextKey error 3648")
			panic(r)
		}
	}()
	if !orderf.isopen {
		return []byte{}, false
	}
	for true {
		orderf.ordermu.RLock()
		rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
		rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
		rootboattype := rootboatoffset & 1
		var result []byte
		var bresult bool
		var bmovenext bool
		if rootboattype == 0 {
			if curkey == nil && rootboat[0]&1 != 0 {
				result = []byte{}
				bresult = true
				bmovenext = false
			} else {
				result, bresult, bmovenext = orderf.nextNextKey(rootboat, curkey, 0)
			}
		}
		orderf.ordermu.RUnlock()
		if bmovenext == false {
			return result, bresult
			// orderf.markmu.Lock()
			// _, bdelload := orderf.markrmmap.Load(string(result))
			// orderf.markmu.Unlock()
			// if bdelload == false {
			// 	return result, bresult
			// }
			// curkey = result
			// continue
		} else {
			return []byte{}, false
		}
	}
	return []byte{}, false
}

func (orderf *OrderFile) nextPreviousKey(boat, sentence []byte, sentencecuri int) (foundkey []byte, bfound, bmovetonext bool) {
	boatcharlen, boatcharoffset := boatCharLen(boat)
	var samecnt uint64
	for sentencecuri < len(sentence) && samecnt < boatcharlen && sentence[sentencecuri] == boat[boatcharoffset+samecnt] {
		samecnt++
		sentencecuri++
	}
	//fmt.Println(orderf.path,"nextNextKey:", samecnt, boatcharlen, samecnt, sentencecuri, sentence, boat)
	if boat[0]&1 == 1 {
		if sentencecuri == len(sentence) {
			foundkey := BytesCombine(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+(boatcharlen-samecnt)])
			if bytes.Compare(foundkey, sentence) < 0 {
				//fmt.Println(orderf.path,"2485 return")
				return foundkey, true, false
			} else {
				//fmt.Println(orderf.path,"2488 56 return", foundkey)
				return []byte{}, false, true
			}
		}
	}

	var foundind, mofind, mofcmpval int = -1, -1, -1
	if getBoatChildLen(boat) > 0 && samecnt == boatcharlen {
		foundind, mofind, mofcmpval = orderf.quickFindBuf(boat[boatcharoffset+boatcharlen+1:], sentence, sentencecuri)
	}
	//fmt.Println(orderf.path,"2498 foundind:", foundind, "sentence match:", string(sentence[:sentencecuri]), string(sentence))
	if foundind == -1 && mofind != -1 && boatcharlen == samecnt {
		if mofcmpval <= 0 {
			if mofind+1 < int(getBoatChildLen(boat))/6 {
				foundind = mofind + 1
			} else {
				foundind = mofind
			}
			//fmt.Println(orderf.path,"2502 foundind = mofind + 1", foundind, mofind, boat)
		} else {
			foundind = mofind
			//fmt.Println(orderf.path,"2505 foundind = mofind", foundind, mofind, boat)
		}
	}
	if int(foundind) >= 0 && int(foundind) < int(getBoatChildLen(boat))/6 && samecnt != boatcharlen && samecnt > 0 {
		sentence = sentence[:sentencecuri]
		//fmt.Println(orderf.path,"sentencecuri", sentencecuri, "sentence", string(sentence), []byte(sentence))
		sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
		sentencecuri += int(boatcharlen - samecnt)
		//fmt.Println(orderf.path,"sentencecuri2", sentencecuri, "sentence", string(sentence), []byte(sentence))
		samecnt = boatcharlen
		foundind = int(getBoatChildLen(boat))/6 - 1
	}

	if foundind == -1 && getBoatChildLen(boat) > 0 && sentencecuri+int(boatcharlen-samecnt) <= len(sentence) && bytes.Compare(boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen], sentence[sentencecuri:sentencecuri+int(boatcharlen-samecnt)]) <= 0 {
		sentence = sentence[:sentencecuri]
		sentence = append(sentence, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
		sentencecuri += int(boatcharlen - samecnt)
		samecnt = boatcharlen

		foundind = int(getBoatChildLen(boat))/6 - 1
	}

	//fmt.Println(orderf.path,"nextNextKey foundind", foundind, "childcnt:", int(getBoatChildLen(boat))/6, "samecnt:", samecnt, "sentencecuri", sentencecuri, "sentence:", string(sentence), boat)
	for int(foundind) >= 0 && int(foundind) < int(getBoatChildLen(boat))/6 {
		makeoffsetbt := make([]byte, 8)
		//fmt.Println(orderf.path,"foundind:", foundind, "childcnt:", int(getBoatChildLen(boat))/6, boatcharoffset, boatcharlen, boat)
		copy(makeoffsetbt[2:], boat[boatcharoffset+boatcharlen+1+uint64(foundind)*6:boatcharoffset+boatcharlen+1+uint64(foundind)*6+6])
		nextoffset2 := binary.BigEndian.Uint64(makeoffsetbt)
		//fmt.Println(orderf.path,"firstchar compare:", string([]byte{byte(nextoffset2 >> 40)}), string(sentence[sentencecuri:sentencecuri+1]))
		wordtypeval2 := nextoffset2 & 1
		if sentencecuri < len(sentence) && byte(nextoffset2>>40) == sentence[sentencecuri] {
			switch wordtypeval2 {
			case 0:
				sentencecuri++
				nextoffset2 = nextoffset2 & 0xFFFFFFFFFF
				nextoffset2id := nextoffset2 >> 1
				subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
				fndkey, bfndkey, bmovenext := orderf.nextPreviousKey(subboat, sentence, sentencecuri)
				//fmt.Println(orderf.path,"2524 fndkey, bfndkey, bmovenext", fndkey, bfndkey, bmovenext, "sentence", string(sentence), subboat)
				curendkey := []byte{}
				if bmovenext && subboat[0]&1 == 1 {
					boatcharlen, boatcharoffset := boatCharLen(subboat)
					curendkey = BytesCombine(sentence[:sentencecuri], subboat[boatcharoffset:boatcharoffset+boatcharlen])
					//fmt.Println(orderf.path,"2529 curendkey:", curendkey, boat, subboat, "sentence", string(sentence))
				}
				curendkey2 := []byte{}
				if bmovenext && subboat[0]&1 == 1 && boat[0]&1 == 1 {
					boatcharlen2, boatcharoffset2 := boatCharLen(subboat)
					curendkey2 = BytesCombine(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+(boatcharlen-samecnt)], subboat[boatcharoffset2:boatcharoffset2+boatcharlen2])
					//fmt.Println(orderf.path,"3202 curendkey2", curendkey2)
				}
				if bmovenext && subboat[0]&1 == 1 && bytes.Compare(curendkey, sentence) < 0 {
					//fmt.Println(orderf.path,"2538 return", subboat, string(subboat), string(curendkey), "6666", string(sentence))
					return curendkey, true, false
				} else if bmovenext && subboat[0]&1 == 1 && boat[0]&1 == 1 && bytes.Compare(curendkey2, sentence) < 0 {
					//fmt.Println(orderf.path,"2541 return", "subboat:", subboat, "boat:", boat)
					return curendkey2, true, false
				} else if bmovenext {
					//fmt.Println(orderf.path,"6666666666666666666666")
					sentencecuri -= 1
					//fmt.Println(orderf.path,"2527 int(foundind)-1 :", int(foundind)-1, boat[0], boat)
					if int(foundind)-1 >= 0 {
						foundind = foundind - 1
						continue
					} else {
						return []byte{}, false, true
					}
				} else {
					//fmt.Println(orderf.path,"2535 return", subboat, fndkey)
					return fndkey, bfndkey, false
				}
			case 1:
				v3btn := make([]byte, 8)
				binary.BigEndian.PutUint64(v3btn, nextoffset2)
				v3btnlen := v3btn[7] >> 1
				foundkey := BytesCombine(sentence[:sentencecuri], v3btn[2:2+v3btnlen])
				//fmt.Println(orderf.path,"foundkey, sentence", foundkey, sentence, boat)
				if bytes.Compare(foundkey, sentence) < 0 {
					//fmt.Println(orderf.path,"2600 3 return:", foundkey, true, false)
					return foundkey, true, false
				} else {
					if uint32(foundind)-1 >= 0 {
						//fmt.Println(orderf.path,"3 return2")
						foundind = foundind - 1
						continue
					} else if boat[0]&1 == 1 {
						//fmt.Println(orderf.path,"2603 return4")
						return BytesCombine(sentence[:sentencecuri], boat[boatcharoffset+samecnt:boatcharoffset+samecnt+(boatcharlen-samecnt)]), true, false
					} else {
						//fmt.Println(orderf.path,"3 return3")
						return []byte{}, false, true
					}
				}
			}
		} else if sentencecuri == len(sentence) || sentencecuri < len(sentence) && byte(nextoffset2>>40) < sentence[sentencecuri] || boatcharlen != samecnt {
			nearchar := []byte{}
			nearchar = append(nearchar, sentence[0:sentencecuri]...)
			if boatcharlen != samecnt {
				nearchar = append(nearchar, boat[boatcharoffset+samecnt:boatcharoffset+boatcharlen]...)
			}
			for true {
				nearchar = append(nearchar, byte(nextoffset2>>40))
				nextoffset3 := nextoffset2 & 0xFFFFFFFFFF
				wordtypeval2 = nextoffset3 & 1
				nextoffset2id := nextoffset3 >> 1
				switch wordtypeval2 {
				case 0:
					//fmt.Println(orderf.path,"nextoffset2id:", nextoffset2id)
					subboat := orderf.readFullBoatNoZip(uint64(nextoffset2id))
					//fmt.Println(orderf.path,"nextoffset2id2:", nextoffset2id, subboat, subboat[0]&1)
					boatcharlen, boatcharoffset = boatCharLen(subboat)
					//fmt.Println(orderf.path,"boatcharlen, boatcharoffset:", boatcharlen, boatcharoffset)
					nearchar = append(nearchar, subboat[boatcharoffset:boatcharoffset+boatcharlen]...)
					if subboat[0]&1 == 1 && getBoatChildLen(subboat) == 0 {
						//fmt.Println(orderf.path,"2576 return")
						return nearchar, true, false
					} else {
						if getBoatChildLen(subboat) > 0 {
							makeoffsetbt := make([]byte, 8)
							copy(makeoffsetbt[2:], subboat[boatcharoffset+boatcharlen+1+uint64(getBoatChildLen(subboat)/6-1)*6:boatcharoffset+boatcharlen+1+uint64(getBoatChildLen(subboat)/6-1)*6+6])
							nextoffset2 = binary.BigEndian.Uint64(makeoffsetbt)
						} else {
							//fmt.Println(orderf.path,"2584 return")
							return []byte{}, false, false
						}
					}
				case 1:
					v3btn := make([]byte, 8)
					binary.BigEndian.PutUint64(v3btn, nextoffset2)
					v3btnlen := v3btn[7] >> 1
					//fmt.Println(orderf.path,"2592 v3btn:", v3btn, v3btnlen)
					nearchar = append(nearchar, v3btn[2+1:2+1+v3btnlen-1]...)
					return nearchar, true, false
				}
			}
		} else {
			if uint32(foundind)-1 >= 0 {
				foundind = foundind - 1
				continue
			} else {
				//fmt.Println(orderf.path,"not found1")
				return []byte{}, false, true
			}
		}
	}
	//fmt.Println(orderf.path,"not found2")
	return []byte{}, false, true
}

//if previous value for delete,should use RealRm function
func (orderf *OrderFile) PreviousKey(curkey []byte) (previouskey []byte, bprevious bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, curkey, "PreviousKey error 3648")
			panic(r)
		}
	}()
	if !orderf.isopen {
		return []byte{}, false
	}

	// runch := make(chan int, 0)
	// runpos := int64(0)
	// runpostime := time.Now().Local().Unix()
	// var runstr string
	// go v4simPreviousKey(string(curkey), runch, &runpos, &runpostime, &runstr, orderf)

	for true {
		orderf.ordermu.RLock()
		rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
		rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
		rootboattype := rootboatoffset & 1
		var result []byte
		var bresult bool
		var bmoveprevious bool
		if rootboattype == 0 {
			//fmt.Println(orderf.path,"rootboat:", orderf.path, rootboat)
			result, bresult, bmoveprevious = orderf.nextPreviousKey(rootboat, curkey, 0)
			if bresult == false && rootboat[0]&1 == 1 && len(curkey) != 0 {
				bresult = true
				bmoveprevious = false
			}
		}
		orderf.ordermu.RUnlock()
		if bmoveprevious == false {
			if len(result) >= len(curkey) && bytes.Compare(result[:len(curkey)], curkey) == 0 {
				curkey2 := BytesCombine(curkey)
				curkey2[len(curkey2)-1] -= 1
				curkey = append(curkey2, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
				continue
			}
			return result, bresult
			// orderf.markmu.Lock()
			// _, bdelload := orderf.markrmmap.Load(string(result))
			// orderf.markmu.Unlock()
			// if bdelload == false {
			// 	//runch <- 1
			// 	return result, bresult
			// }
			// curkey = result
			// continue
		} else {
			//runch <- 1
			return []byte{}, false
		}
	}
	return []byte{}, false
}

//RealRmPush code merge from RealPush RealRm.
func (orderf *OrderFile) RealRmPush(key, value []byte) bool {
	defer func() {
		if er := recover(); er != nil {
			fmt.Println("RealRmPush 4348", key, value)
			panic("RealRmPush error")
		}
	}()
	if !orderf.isopen {
		return false
	}

	if len(key)+len(value) >= (1<<17)-256 {
		if len(key)+len(value) < 5*(1<<17) {
			var cslen int
			if orderf.compresstype == 0 {
				cslen = len(ZipEncode(nil, append(key, value...), 1))
			} else if orderf.compresstype == 1 {
				cslen = len(SnappyEncode(nil, append(key, value...)))
			} else if orderf.compresstype == 2 {
				cslen = len(Lz4Encode(nil, append(key, value...)))
			} else if orderf.compresstype == 3 {
				cslen = len(XzEncode(nil, append(key, value...)))
			} else if orderf.compresstype == 4 {
				cslen = len(FlateEncode(nil, append(key, value...), 1))
			}
			if cslen >= (1 << 17) {
				return false
			}
		} else {
			return false
		}
	}

	if orderf.lastblockendpos > ((1<<36)-32*1024*1024) || orderf.lastblockorderend > ((1<<35)-64*1024*1024) {
		orderfiletool.WriteFile(orderf.path+".reachedmax!", []byte{})
		return false
	}

	orderf.ordermu.Lock()
	//orderf.ErrorRecord("RP", key, value)
	rootboatoffset := binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboat := orderf.readFullBoatNoZip(rootboatoffset >> 1)
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf(orderf.path, key, value, rootboat, rootboatoffset>>1, "RealRmPush 3846", r)
			fmt.Println(runtime.Caller(0))
			panic(r)
		}
	}()
	rootboattype := rootboatoffset & 1
	if rootboattype == 0 {
		if len(key) > 0 {
			needdel, _ := orderf.nextRmKey(rootboatoffset>>1, rootboat, key, 0, true)
			if needdel {
				boatcharlen, boatcharoffset := boatCharLen(rootboat)
				boatstartlen := len(rootboat)
				foundind, _, _ := orderf.quickFindBuf(rootboat[boatcharoffset+boatcharlen+1:], key, 0)
				rootboat = BytesCombine(rootboat[:boatcharoffset+boatcharlen+1+uint64(foundind)*6], rootboat[boatcharoffset+boatcharlen+1+uint64(foundind)*6+6:])
				if rootboat[boatcharlen+boatcharoffset] >= 1 {
					rootboat[boatcharlen+boatcharoffset] -= 1
				} else {
					rootboat[0] = rootboat[0] &^ (4)
					rootboat = rootboat[:boatcharlen+boatcharoffset]
				}
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatstartlen), rootboat)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 0
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
			}
		} else if rootboat[0]&1 != 0 {
			rootboat[0] = rootboat[0] &^ 1
			var retrootboatoffset uint64
			retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(len(rootboat)), rootboat)
			if retrootboatoffset != rootboatoffset>>1 {
				retrootboatoffset = retrootboatoffset<<1 | 0
				tempbt := make([]byte, 8)
				binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
				copy(orderf.shorttable[0:6], tempbt[2:])
			}
			orderf.totalcount -= 1
			orderf.bhavenewwrite = true
		}
	}

	keyval := orderfiletool.BytesCombine(key, value)
	rootboatoffset = binary.BigEndian.Uint64(append([]byte{0, 0}, orderf.shorttable[0:6]...))
	rootboattype = rootboatoffset & 1
	switch rootboattype {
	case 0, 1:
		var boatbt []byte
		boatbt = orderf.readFullBoatNoZip(rootboatoffset >> 1)
		//fmt.Println(orderf.path,"rootboatbt", rootboatoffset>>2, boatbt)
		boatbtstartlen := len(boatbt)
		boatbtcharlen, boatbtcharoffset := boatCharLen(boatbt)
		bfound := false
		wordcuri := 0
		if boatbtcharlen == 0 {
			var foundind int = -1
			if getBoatChildLen(boatbt) > 0 {
				foundind, _, _ = orderf.quickFindBuf(boatbt[boatbtcharoffset+boatbtcharlen+1:], keyval, wordcuri)
			}
			if foundind != -1 {
				curboatfirstchar := keyval[0]
				wordcuri += 1
				nextoffset := binary.BigEndian.Uint64(append(make([]byte, 2), boatbt[boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6:boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6+6]...))
				boattype := nextoffset & 1
				switch boattype {
				case 0:
					if byte(nextoffset>>40) != keyval[0] {
						//fmt.Println(byte(nextoffset>>40), word[0], word)
						panic(orderf.path + ":first word not equal 2075.")
					}
					nextoffset = nextoffset & 0xFFFFFFFFFF
					retnextoffset, _ := orderf.nextPushKey(uint64(nextoffset), keyval, wordcuri)
					if retnextoffset != uint64(nextoffset>>1) {
						if retnextoffset == 0 {
							panic(orderf.path + ":return error 2083.")
						}
						retnextoffset3 := uint64(curboatfirstchar)<<40 | uint64(retnextoffset)<<1 | 0
						retnextoffset3bt := make([]byte, 8)
						binary.BigEndian.PutUint64(retnextoffset3bt, retnextoffset3)
						copy(boatbt[boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6:boatbtcharoffset+boatbtcharlen+1+uint64(foundind)*6+6], retnextoffset3bt[2:])
						var retrootoffset uint64
						retrootoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
						if retrootoffset != uint64(rootboatoffset>>1) {
							tempbt := make([]byte, 8)
							binary.BigEndian.PutUint64(tempbt, retrootoffset<<1|0)
							copy(orderf.shorttable[0:6], tempbt[2:])
						}
					}
				default:
					panic(orderf.path + ":root boat have wrong boat type 2097.")
				}
				bfound = true
			}
		}

		if bfound == false {
			if len(keyval) > 0 {
				boatbtcharlen, boatbtcharoffset := boatCharLen(boatbt)
				var foundpos, mofpos, mofcmpval int = -1, -1, -1
				if getBoatChildLen(boatbt) > 0 {
					//fmt.Println(orderf.path,"boatbt", boatbt)
					foundpos, mofpos, mofcmpval = orderf.quickFindBuf(boatbt[boatbtcharlen+boatbtcharoffset+1:], keyval, 0)
				}
				if orderf.fixkeyendbt != nil {
					orderf.fixkeylen = bytes.Index(keyval, orderf.fixkeyendbt)
					if orderf.fixkeylen == -1 {
						orderf.fixkeylen = 0
					} else {
						orderf.fixkeylen += len(orderf.fixkeyendbt)
					}
				}
				wordlastmem, _ := orderf.buildLastWordMem(keyval, 0, orderf.fixkeylen-0)
				var wordlastmemid uint64
				wordlastmemid = uint64(orderf.writeLastBlockNoZip(wordlastmem))
				//check4Zero(wordlastmem)
				wordlastmemidbt := make([]byte, 8)
				binary.BigEndian.PutUint64(wordlastmemidbt, uint64(keyval[0])<<40|wordlastmemid<<1|0)
				if foundpos != -1 {
					//fmt.Println(orderf.path,"boatbt", boatbt, word)
					panic(orderf.path + ":root boat insert found same first char boat 2120.")
				}
				if mofpos == -1 {
					boatbt = BytesCombine(boatbt, []byte{0}, wordlastmemidbt[2:])
				} else if mofcmpval > 0 {
					boatbt = BytesCombine(boatbt[:boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6+6], wordlastmemidbt[2:], boatbt[boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6+6:])
					boatbt[boatbtcharlen+boatbtcharoffset] += 1
				} else if mofcmpval < 0 {
					boatbt = BytesCombine(boatbt[:boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6], wordlastmemidbt[2:], boatbt[boatbtcharlen+boatbtcharoffset+1+uint64(mofpos)*6:])
					boatbt[boatbtcharlen+boatbtcharoffset] += 1
				}
				boatbt[0] |= 4
				//fmt.Println(orderf.path,"boatbt634", boatbt)
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 0
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
				orderf.totalcount += 1
				orderf.bhavenewwrite = true
			} else {
				if boatbt[0]&1 == 0 {
					orderf.totalcount += 1
					orderf.bhavenewwrite = true
				}
				boatbt[0] |= 1
				var retrootboatoffset uint64
				retrootboatoffset = orderf.writeOldBoatNoZip(rootboatoffset>>1, uint64(boatbtstartlen), boatbt)
				if retrootboatoffset != rootboatoffset>>1 {
					retrootboatoffset = retrootboatoffset<<1 | 1
					tempbt := make([]byte, 8)
					binary.BigEndian.PutUint64(tempbt, retrootboatoffset)
					copy(orderf.shorttable[0:6], tempbt[2:])
				}
			}
		}
	}
	orderf.ordermu.Unlock()
	return true
}

func (orderf *OrderFile) Count() int64 {
	if orderf.totalcount < 0 {
		panic(orderf.path + ":totao count error!")
	}
	return orderf.totalcount
}

func (orderf *OrderFile) GetPath() string {
	return orderf.path
}

func BytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func BytesClone(src []byte) []byte {
	target := make([]byte, len(src), len(src)+8)
	copy(target, src)
	return target
}

func OrderFileClear(path string) {
	os.Remove(path + ".rmpush")
	os.Remove(path + ".rmpushinsave")
	os.Remove(path)
	os.Remove(path + ".head")
	os.Remove(path + ".head.backup")
	os.Remove(path + ".headsave")
	os.Remove(path + ".bfree")
	os.Remove(path + ".bfree.backup")
	os.Remove(path + ".bfreesave")
	os.Remove(path + ".ffree")
	os.Remove(path + ".ffree.backup")
	os.Remove(path + ".ffreesave")
	os.Remove(path + ".rmpushinmem")
	os.Remove(path + ".newsavedata")
	os.Remove(path + ".beopen")
	os.Remove(path + ".rmpushtemp")
	os.Remove(path + ".rmpushtempok")
	os.Remove(path + ".headsaveok")
	os.Remove(path + ".opt")
	os.Remove(path + "_err.log")
	pathls := []string{path + ".rmpush", path + ".rmpushinsave", path, path + ".head", path + ".head.backup", path + ".headsave", path + ".bfree", path + ".bfree.backup", path + ".bfreesave", path + ".ffree", path + ".ffree.backup", path + ".ffreesave", path + ".rmpushinmem", path + ".newsavedata", path + ".beopen", path + ".rmpushtemp", path + ".rmpushtempok", path + ".headsaveok", path + ".opt"}
	for _, pa := range pathls {
		_, rmpushe := os.Stat(pa)
		if rmpushe == nil {
			panic(path + " clear " + pa + " error.Maybe have handle no close.")
		}
	}
}

func Backup(path string) {
	OrderFileClear(orderfiletool.FilePathAppendDir(path, "backup"))
	if orderfiletool.FileSize(path) < 1024*1024*1024 {
		orderfiletool.CopyFile(path, orderfiletool.FilePathAppendDir(path, "backup"))
	}
	orderfiletool.CopyFile(path+".head", orderfiletool.FilePathAppendDir(path+".head", "backup"))
	orderfiletool.CopyFile(path+".head.backup", orderfiletool.FilePathAppendDir(path+".head.backup", "backup"))
	orderfiletool.CopyFile(path+".headsave", orderfiletool.FilePathAppendDir(path+".headsave", "backup"))
	orderfiletool.CopyFile(path+".bfree", orderfiletool.FilePathAppendDir(path+".bfree", "backup"))
	orderfiletool.CopyFile(path+".bfree.backup", orderfiletool.FilePathAppendDir(path+".bfree.backup", "backup"))
	orderfiletool.CopyFile(path+".bfreesave", orderfiletool.FilePathAppendDir(path+".bfreesave", "backup"))
	orderfiletool.CopyFile(path+".ffree", orderfiletool.FilePathAppendDir(path+".ffree", "backup"))
	orderfiletool.CopyFile(path+".ffree.backup", orderfiletool.FilePathAppendDir(path+".ffree.backup", "backup"))
	orderfiletool.CopyFile(path+".ffreesave", orderfiletool.FilePathAppendDir(path+".ffreesave", "backup"))
	orderfiletool.CopyFile(path+".rmpush", orderfiletool.FilePathAppendDir(path+".rmpush", "backup"))
	orderfiletool.CopyFile(path+".rmpushinsave", orderfiletool.FilePathAppendDir(path+".rmpushinsave", "backup"))
	orderfiletool.CopyFile(path+".rmpushinmem", orderfiletool.FilePathAppendDir(path+".rmpushinmem", "backup"))
	orderfiletool.CopyFile(path+".newsavedata", orderfiletool.FilePathAppendDir(path+".newsavedata", "backup"))
	orderfiletool.CopyFile(path+".headsaveok", orderfiletool.FilePathAppendDir(path+".headsaveok", "backup"))
	orderfiletool.CopyFile(path+".beopen", orderfiletool.FilePathAppendDir(path+".beopen", "backup"))
	orderfiletool.CopyFile(path+".rmpushtemp", orderfiletool.FilePathAppendDir(path+".rmpushtemp", "backup"))
	orderfiletool.CopyFile(path+".rmpushtempok", orderfiletool.FilePathAppendDir(path+".rmpushtempok", "backup"))
}
func MoveToOld(path string) {
	os.MkdirAll(path, 0666)
	OrderFileClear(orderfiletool.FilePathAppendDir(path, "old"))
	os.Rename(path, orderfiletool.FilePathAppendDir(path, "old"))
	os.Rename(path+".head", orderfiletool.FilePathAppendDir(path+".head", "old"))
	os.Rename(path+".head.backup", orderfiletool.FilePathAppendDir(path+".head.backup", "old"))
	os.Rename(path+".headsave", orderfiletool.FilePathAppendDir(path+".headsave", "old"))
	os.Rename(path+".bfree", orderfiletool.FilePathAppendDir(path+".bfree", "old"))
	os.Rename(path+".bfree.backup", orderfiletool.FilePathAppendDir(path+".bfree.backup", "old"))
	os.Rename(path+".bfreesave", orderfiletool.FilePathAppendDir(path+".bfreesave", "old"))
	os.Rename(path+".ffree", orderfiletool.FilePathAppendDir(path+".ffree", "old"))
	os.Rename(path+".ffree.backup", orderfiletool.FilePathAppendDir(path+".ffree.backup", "old"))
	os.Rename(path+".ffreesave", orderfiletool.FilePathAppendDir(path+".ffreesave", "old"))
	os.Rename(path+".rmpush", orderfiletool.FilePathAppendDir(path+".rmpush", "old"))
	os.Rename(path+".rmpushinsave", orderfiletool.FilePathAppendDir(path+".rmpushinsave", "old"))
	os.Rename(path+".rmpushinmem", orderfiletool.FilePathAppendDir(path+".rmpushinmem", "old"))
	os.Rename(path+".newsavedata", orderfiletool.FilePathAppendDir(path+".newsavedata", "old"))
	os.Rename(path+".headsaveok", orderfiletool.FilePathAppendDir(path+".headsaveok", "old"))
	os.Rename(path+".beopen", orderfiletool.FilePathAppendDir(path+".beopen", "old"))
	os.Rename(path+".rmpushtemp", orderfiletool.FilePathAppendDir(path+".rmpushtemp", "old"))
	os.Rename(path+".rmpushtempok", orderfiletool.FilePathAppendDir(path+".rmpushtempok", "old"))
	os.Rename(path+".opt", orderfiletool.FilePathAppendDir(path+".opt", "old"))
}

func Rename(pathfrom, pathto string) {
	orderfiletool.MakePathDirExists(pathto)
	os.Rename(pathfrom, pathto)
	os.Rename(pathfrom+".head", pathto+".head")
	os.Rename(pathfrom+".head.backup", pathto+".head.backup")
	os.Rename(pathfrom+".headsave", pathto+".headsave")
	os.Rename(pathfrom+".bfree", pathto+".bfree")
	os.Rename(pathfrom+".bfree.backup", pathto+".bfree.backup")
	os.Rename(pathfrom+".bfreesave", pathto+".bfreesave")
	os.Rename(pathfrom+".ffree", pathto+".ffree")
	os.Rename(pathfrom+".ffree.backup", pathto+".ffree.backup")
	os.Rename(pathfrom+".ffreesave", pathto+".ffreesave")
	os.Rename(pathfrom+".rmpush", pathto+".rmpush")
	os.Rename(pathfrom+".rmpushinsave", pathto+".rmpushinsave")
	os.Rename(pathfrom+".rmpushinmem", pathto+".rmpushinmem")
	os.Rename(pathfrom+".newsavedata", pathto+".newsavedata")
	os.Rename(pathfrom+".headsaveok", pathto+".headsaveok")
	os.Rename(pathfrom+".beopen", pathto+".beopen")
	os.Rename(pathfrom+".rmpushtemp", pathto+".rmpushtemp")
	os.Rename(pathfrom+".rmpushtempok", pathto+".rmpushtempok")
	os.Rename(pathfrom+".opt", pathto+".opt")
}

func CopyFile(src, target string) bool {
	oldrmpushf, oldrmpushferr := os.OpenFile(src, os.O_RDWR, 0666)
	if oldrmpushferr == nil {
		compactrmpushf, _ := os.Create(target)
		endpos, _ := oldrmpushf.Seek(0, os.SEEK_END)
		if endpos > 0 {
			readbuf := make([]byte, 2*1024*1024)
			oldrmpushf.Seek(0, os.SEEK_SET)
			for i := int64(0); i < endpos; {
				readlen := int64(len(readbuf))
				if i+readlen > endpos {
					readlen = endpos - i
				}
				_, readerr := oldrmpushf.Read(readbuf[:readlen])
				if readerr != nil {
					compactrmpushf.Close()
					oldrmpushf.Close()
					return false
				}
				compactrmpushf.Write(readbuf[:readlen])
				i += readlen
			}
		}
		compactrmpushf.Close()
		oldrmpushf.Close()
	} else {
		return false
	}
	return true
}
