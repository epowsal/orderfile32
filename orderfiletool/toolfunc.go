package orderfiletool

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

func StdPath(path string) string {
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	return path
}

func StdUnixLikePath(path string) string {
	re := regexp.MustCompile("[/\\\\]+")
	return re.ReplaceAllString(path, "/")
}

func ToAbsolutePath(path string) string {
	if path == "" {
		return ""
	}
	var bsepend bool
	if path[len(path)-1] == '/' || path[len(path)-1] == '\\' {
		bsepend = true
	}
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\\\", "\\", -1)
	path = strings.Replace(path, "\\", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	path = strings.Replace(path, "//", "/", -1)
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	curdir := ""
	if runtime.GOOS == "windows" {
		if path[1] != ':' {
			curdir = CurDir()
		}
	} else if path[0] != '/' {
		curdir = CurDir()
	}
	curdirls := strings.Split(curdir, "/")
	if len(curdirls) > 0 {
		curdirls = curdirls[:len(curdirls)-1]
	}
	if path[0] == '/' || path[1] == ':' {
		if len(curdirls) > 0 {
			curdirls = curdirls[:1]
		}
	}

	pathls := strings.Split(path, "/")
	for i := 0; i < len(pathls); i++ {
		if pathls[i] == "." || pathls[i] == "" {

		} else if pathls[i] == ".." {
			if len(pathls)-1 >= 0 {
				curdirls = curdirls[:len(curdirls)-1]
			}
		} else {
			curdirls = append(curdirls, pathls[i])
		}
	}
	okpath := strings.Join(curdirls, "/")
	if bsepend {
		okpath += "/"
	}
	return okpath
}

func ReadFile(filepath string) ([]byte, error) {
	ff, ffe := os.OpenFile(filepath, os.O_RDONLY, 0666)
	if ffe != nil {
		return nil, ffe
	}
	defer ff.Close()
	endpos, endpose := ff.Seek(0, os.SEEK_END)
	if endpose != nil {
		return nil, ffe
	}
	startpos, startpose := ff.Seek(0, os.SEEK_SET)
	if startpose != nil || startpos != 0 {
		return nil, ffe
	}
	buf := make([]byte, endpos)
	rcnt, err := io.ReadFull(ff, buf)
	if err != nil || int64(rcnt) != endpos {
		return nil, err
	}
	return buf, nil
}

func SaveFile(filepath string, data []byte) error {
	return WriteFile(filepath, data)
}

func WriteFile(filepath string, data []byte) error {
	//ff, ffe := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0666)
	ff, ffe := os.Create(filepath)
	if ffe != nil {
		return ffe
	}
	defer ff.Close()
	_, we := ff.Write(data)
	if we != nil {
		return we
	}
	synce := ff.Sync()
	if synce != nil {
		return synce
	}
	ff.Close()
	return nil
}

func CheckZero(bt []byte) bool {
	for i := 0; i < len(bt); i++ {
		if bt[i] != 0 {
			return false
		}
	}
	return true
}

func FilePathReplaceDir(filepath, dirname string) string {
	filepath = StdPath(filepath)
	filename := filepath[strings.LastIndex(filepath, "/")+1:]
	filepath = filepath[:strings.LastIndex(filepath, "/")]
	return filepath[:strings.LastIndex(filepath, "/")+1] + dirname + "/" + filename
}

func FilePathAppendDir(filepath, dirname string) string {
	filepath = StdPath(filepath)
	if strings.LastIndex(filepath, "/") != -1 {
		return filepath[:strings.LastIndex(filepath, "/")+1] + dirname + "/" + filepath[strings.LastIndex(filepath, "/")+1:]
	} else {
		return dirname + "/" + filepath
	}
}

func CurDir() string {
	appdir, _ := os.Getwd()
	appdir = strings.Replace(appdir, "\\", "/", -1)
	if appdir[len(appdir)-1] != '/' {
		appdir += "/"
	}
	return appdir
}

func CopyFile(src, target string) bool {
	target = strings.Replace(target, "\\", "/", -1)
	oldrmpushf, oldrmpushferr := os.OpenFile(src, os.O_RDWR, 0666)
	if oldrmpushferr == nil {
		if strings.LastIndex(target, "/") != -1 {
			targetdir := target[:strings.LastIndex(target, "/")]
			os.MkdirAll(targetdir, 0666)
		}
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

func GetGoRoutineID() int {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("panic recover:panic info:%V", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func BytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func BytesJoin(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func FileSize(filepath string) int64 {
	dl, dler := os.Stat(filepath)
	if dler == nil {
		if !dl.IsDir() {
			ff, ffer := os.OpenFile(filepath, os.O_RDONLY, 0666)
			if ffer == nil {
				endpos, sker := ff.Seek(0, os.SEEK_END)
				ff.Close()
				if sker == nil {
					return endpos
				}
			}
		}
	}
	return -1
}

func MakeDir(path string) bool {
	return MakePathDirExists(path)
}

func MakePathDirExists(path string) bool {
	path = ToAbsolutePath(path)
	dirnames := regexp.MustCompile("[\\\\/]+").Split(path, -1)
	if dirnames[len(dirnames)-1] != "" {
		dirnames = dirnames[:len(dirnames)-1]
	}
	ppath := ""
	for i := 0; i < len(dirnames); i++ {
		if ppath == "" {
			ppath += dirnames[i]
		} else {
			ppath += "/" + dirnames[i]
		}
		os.Mkdir(ppath, 0666)
	}
	return true
}

func RandPrintChar(min, max int) (outbt []byte) {
	sublen := min + rand.Intn(max-min+1)
	for i := 0; i < sublen; i++ {
		outbt = append(outbt, byte(0x20+rand.Intn(0x7e-0x20)))
	}
	return outbt
}

func SliceSearch(set interface{}, val interface{}, from int) int {
	switch set.(type) {
	case []int:
		{
			for i := from; i < len(set.([]int)); i++ {
				if val.(int) == set.([]int)[i] {
					return i
				}
			}
		}
	case []int8:
		{
			for i := from; i < len(set.([]int8)); i++ {
				if val.(int8) == set.([]int8)[i] {
					return i
				}
			}
		}
	case []int16:
		{
			for i := from; i < len(set.([]int16)); i++ {
				if val.(int16) == set.([]int16)[i] {
					return i
				}
			}
		}
	case []int32:
		{
			for i := from; i < len(set.([]int32)); i++ {
				if val.(int32) == set.([]int32)[i] {
					return i
				}
			}
		}
	case []int64:
		{
			for i := from; i < len(set.([]int64)); i++ {
				if val.(int64) == set.([]int64)[i] {
					return i
				}
			}
		}
	case []uint8:
		{
			for i := from; i < len(set.([]uint8)); i++ {
				if val.(uint8) == set.([]uint8)[i] {
					return i
				}
			}
		}
	case []uint16:
		{
			for i := from; i < len(set.([]uint16)); i++ {
				if val.(uint16) == set.([]uint16)[i] {
					return i
				}
			}
		}
	case []uint32:
		{
			for i := from; i < len(set.([]uint32)); i++ {
				if val.(uint32) == set.([]uint32)[i] {
					return i
				}
			}
		}
	case []uint64:
		{
			for i := from; i < len(set.([]uint64)); i++ {
				if val.(uint64) == set.([]uint64)[i] {
					return i
				}
			}
		}
	case []string:
		{
			for i := from; i < len(set.([]string)); i++ {
				if val.(string) == set.([]string)[i] {
					return i
				}
			}
		}
	case [][]byte:
		{
			for i := from; i < len(set.([][]byte)); i++ {
				if bytes.Compare(val.([]byte), set.([][]byte)[i]) == 0 {
					return i
				}
			}
		}
	}
	return -1
}
