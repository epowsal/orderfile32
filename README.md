# orderfile32

![iconsmall](https://user-images.githubusercontent.com/89308109/133926499-6cb97519-1e4f-454e-98fa-3e9a941dec77.png)

pure golang key database support key have value

The orderfile32 is standard alone fast key value database. It have two version. one is this. two is orderfilepeta. this version is open source version. this version support database size is 32gb. max key add value length is 128kb. orderfilepeta database max size support 16peta byte. max key add value length 32mb. orderfile peta for commerce use only. if you want orderfilepeta you can contact me by email iwlb@outlook.com. normal user orderfile32 is enough for using. 32gb is compressed size. this size can contain about 2billion normal item data.

this project I devloped many years.  now I release it for public. if you like it. you can use it. you will found it is very good.


# install

got get github.com/epowsal/orderfile32


# simple example code

```go
package main
import "github.com/epowsal/orderfile32"
db,dber:=NewOrderfile("myfirstdb",0,0,[]byte{0})
if dber!=nil {
  panic(dber)
 }
 db.PushKey([]byte("key"),[]byte("value"))
 fkey,bfkey:=db.FillKey([]byte("key"))
 if bfkey==false {
    panic("error")
 }
 fmt.Println(fkey)
 db.Close()
```
# thanks and support me by donate

paypal:


[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PAGZ8VVGG67XY)


# 捐赠以表示感谢与支持

支付宝：

![接受捐赠200pixel](https://user-images.githubusercontent.com/89308109/132255156-1926b435-d628-40a8-89a2-1682f2e69a69.png)



