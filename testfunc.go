package orderfile32

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func wordindcmp(wordind1, wordind2 []byte) bool {
	if len(wordind1) != len(wordind2) {
		return false
	}
	for i := 0; i < len(wordind1); i++ {
		if wordind1[i] != wordind2[i] {
			return false
		}
	}
	return true
}

func genword() []byte {
	wordbt := make([]byte, 0)
	sublen := rand.Intn(18)
	for i := 0; i < 3+sublen; i++ {
		wordbt = append(wordbt, byte(0x41+rand.Intn(26)))
	}
	return wordbt
}

func Float32ToByte(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, bits)
	return bytes
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func genfloat32bt() []byte {
	for true {
		f32 := rand.Float32()
		if f32 > 0 {
			return []byte("price_" + string(Float32ToByte(f32)))
		}
	}
	return []byte{}
}

func genword2() []byte {
	wordbt := make([]byte, 3)
	wordbt[0] = 'b'
	wordbt[1] = 'b'
	wordbt[2] = 'c'
	for i := 0; i < 3+rand.Intn(5); i++ {
		wordbt = append(wordbt, byte(0x41+rand.Intn(26)))
	}
	return wordbt
}

func genwordlarge() []byte {
	wordbt := make([]byte, 0)
	genlen := 1024 + rand.Intn(512)
	for i := 0; i < genlen; i++ {
		wordbt = append(wordbt, byte(0x41+rand.Intn(26)))
	}
	return wordbt
}

func genupperstr() string {
	wordbt := make([]byte, 0)
	genlen := 2 + rand.Intn(5)
	for i := 0; i < genlen; i++ {
		wordbt = append(wordbt, byte(0x41+rand.Intn(26)))
	}
	return string(wordbt)
}

func genlowerstr() string {
	wordbt := make([]byte, 0)
	genlen := 2 + rand.Intn(5)
	for i := 0; i < genlen; i++ {
		wordbt = append(wordbt, byte(0x61+rand.Intn(26)))
	}
	return string(wordbt)
}

func LogInit() {
	fmt.Println(binary.BigEndian.Uint32(make([]byte, 4)))
	ioutil.ReadFile("sdfdsf")
	os.Stat("jdslkfj")
	bufio.NewReader(strings.NewReader("dslfk"))
	time.Now().Unix()
	strconv.FormatInt(344, 10)
	fmt.Println(runtime.Caller(0))
	sort.Strings([]string{})
	fmt.Println(bytes.Index([]byte(""), []byte{}))
	fmt.Println(&sync.Map{})
}
