# orderfile32
pure golang key database support key have value

The orderfile32 is standard alone fast key value database. It have two version. one is this. two is orderfilepeta. this version is open source version. this version support database size is 32gb. max key add value length is 128kb. orderfilepeta database max size support 16peta byte. max key add value length 32mb. orderfile peta for commerce use only. if you want orderfilepeta you can contact me by email iwlb@outlook.com. normal user orderfile32 is enough for using. 32gb is compressed size. this size can contain about 2billion normal item data.

this project I devloped many years.  now I release it for public. if you like it. you can use it. you will found it is very good.

install
got get github.com/epowsal/orderfile32

simple example code

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


benchmark compare to other
write benchmark
ordefile32 write 1 million speed test screenshot:
![orderfile321millionwrite_ok](https://user-images.githubusercontent.com/89308109/132252057-2b5a2db1-1a17-477b-b6cf-55398b209d92.png)

badger write 1 million speed test screenshot:
![badger1millinwrite3](https://user-images.githubusercontent.com/89308109/132252146-2665c9a0-0262-4aea-a4a1-1b6ce3313bd5.png)

bolt write 200 thousand speed test screenshot:
![boltwrite200thousand2](https://user-images.githubusercontent.com/89308109/132252209-65c9fba6-9a8c-4ac9-887b-5efd471683ca.png)

orderfilepeta write 1 million speed test screenshot:
![orderfilepeta1millionwrite_ok](https://user-images.githubusercontent.com/89308109/132252496-f2679092-d3b8-4797-88fc-93841304fae6.png)


orderfile32 write 1 million item use 7.1 second.
badger write 1 million item use 26.7 second.
bolt write 0.2 million item use 90 second.
orderfilepeta write 1 million item use 15.9 second.
so orderfile32 if the fastest writing database.the large database orderfilepeta is fast than badger.




read benchmark

ordefile32 read 1 million times speed test screenshot:

![orderfile1millionread_ok](https://user-images.githubusercontent.com/89308109/132252648-ddee3fbb-22c0-4842-8c2f-6c060417d3a6.png)

badger read 1 million times speed test screenshot:
![badger1millinread](https://user-images.githubusercontent.com/89308109/132252718-1d84314f-d6ba-4e0e-8c3b-49e15f9e63e8.png)


bolt read 0.2 million times speed test screenshot:
![bolt200thousandread](https://user-images.githubusercontent.com/89308109/132252788-72817324-3438-4d85-9346-d02a9ff8e68f.png)


ordefilepeta read 1 million times speed test screenshot:
![orderfilepeta1millionread_ok](https://user-images.githubusercontent.com/89308109/132252840-3da20e92-c003-4d83-a868-c9b2176e4a26.png)


after test get:
orderfile32 read 1 million items first time read use 3.9 second. second time read use 3.0 second.
badger read 1 million items use 6.5 second.
bolt read 0.2 million items use 0.5 second
orderfilepeta read 1 million items first read
use 6.5 second. second read use 6.1 second.

so if bolt read 1 million item will use 2.5 second.
so bolt is the fastest reading database.
compare orderfile32 with badger.orderfile32 is almost two times faster than badger.
compare orderfilepeta with badger. they almost equal.


database size compare:
orderfile32 1 million item file size screenshot:
![orderfile32dbfilesize](https://user-images.githubusercontent.com/89308109/132254467-af67856c-2711-4029-bb0f-41c697d881fe.png)

badger 1 million item file size screenshot:
![badgrdbfilesize](https://user-images.githubusercontent.com/89308109/132254489-8b605fd6-3339-4787-8fcc-f6d9ae62295b.png)

bolt 0.2 million item file size screenshot:
![boltdb0 2millionsize](https://user-images.githubusercontent.com/89308109/132254540-56640ca8-b5bc-45ba-99e4-ac16fbbd259d.png)

orderfilepeta 1 million item file size scrrenshot:
![orderfilepetadbfilesize](https://user-images.githubusercontent.com/89308109/132254583-ebbaaafe-03bd-4d57-846f-f86ad1d6dea0.png)


orderfile32 1 million items database file size is 19mb.
badger 1 million items database file size is 26.7mb.
bolt 0.2 million items database file size is 16bm.
orderfilepeta 1 million items database file size if 20mb.

bolt 5 times size is 80mb.
so bolt have largest database file size.
orderfile32 and orderfilepeta have smallest database file size.


thanks and support me by donate
paypal
```html
<form action="https://www.paypal.com/cgi-bin/webscr" method="post" target="_top">
<input type="hidden" name="cmd" value="_s-xclick">
<input type="hidden" name="hosted_button_id" value="6P5BWRBCKHHJN">
<table>
<tr><td><input type="hidden" name="on0" value="Donate Money">Donate Money</td></tr><tr><td><select name="os0">
	<option value="Donate 1 dollar">Donate 1 dollar $ 1.00 USD</option>
	<option value="Donate 10 dollar">Donate 10 dollar $ 10.00 USD</option>
	<option value="Donate 100 dollar">Donate 100 dollar $ 100.00 USD</option>
	<option value="Donate 1000 dollar">Donate 1000 dollar $ 1,000.00 USD</option>
	<option value="Donate 10000 dollar">Donate 10000 dollar $ 10,000.00 USD</option>
</select> </td></tr>
</table>
<input type="hidden" name="currency_code" value="USD">
<input type="image" src="https://www.paypalobjects.com/zh_XC/C2/i/btn/btn_buynowCC_LG.gif" border="0" name="submit" alt="PayPal——最安全便捷的在线支付方式！">
<img alt="" border="0" src="https://www.paypalobjects.com/zh_XC/i/scr/pixel.gif" width="1" height="1">
</form>
```




捐赠以表示感谢与支持：

支付宝
![接受捐赠200pixel](https://user-images.githubusercontent.com/89308109/132255156-1926b435-d628-40a8-89a2-1682f2e69a69.png)





