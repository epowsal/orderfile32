package sysidlemem

import (
	"C"
)

/*
#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
long long int GetIdleMem()  {
    MEMORYSTATUS ms;     //记录内容空间信息的结构体变量
    GlobalMemoryStatus(&ms);//调用GlobalMemoryStatus()函数获取内存信息
	return (long long int)ms.dwAvailPhys;
}
#else
#include <sys/sysinfo.h>
unsigned long GetIdleMem(){
	struct sysinfo info;
	int aaa=sysinfo(&info);
	return (unsigned long)info.freeram;
}
#endif
*/
import "C"

func GetSysIdleMem() int64 {
	return int64(C.GetIdleMem())
}
