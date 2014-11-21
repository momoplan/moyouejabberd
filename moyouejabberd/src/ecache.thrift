namespace java com.cc14514.ecache
  
/**
*函数
*/
service Ecache {  
	/**
	* 输入数组作为指令，例如 ["set","key1","value1"]
	* 返回输入作为输出，例如 ["get","key1"] -> ["value1"]
	*/
	list<string> cmd(1:list<string> request)
}  

