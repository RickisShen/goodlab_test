package main


import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	//"strconv"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	//"google.golang.org/grpc/reflection"
	crand "crypto/rand"
	"math/big"
	"math/rand"
	"strconv"

	KV "main/grpc/mykv"
	//Per "main/persister"
	//"math/rand"
)
// syh：导入包


type Clerk struct {
	servers []string	// You will have to modify this struct.
	leaderId int
	id       int64
	seq      int64
}
// syh：定义结构体Clerk,包含四个字段，后面是各字段的类型


func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
    // syh：":="表示将右边变量的值与类型赋给左边，若用"="则需要提前定义左边的类型
    // syh："big.NewInt"函数用于创建一个大整数，其初始值为 2^62，注意函数返回的是指向这个大整数的指针
	bigx, _ := crand.Int(crand.Reader, max)
    // syh：随机生成一个不大于max的数bigx
	x := bigx.Int64()
    // 将bigx转化为64位整数
	return x
}
// syh：定义函数makeseed（），返回一个随机整数


func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = makeSeed()
	// syh：调用makeseed()函数，生成的ck.id用作客户端的唯一标识
	ck.seq = 0
	// syh：用于追踪请求的序列号
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}
// syh：接受服务器地址作为参数，初始化各个字段产生一个Clerk，并返回它的指针


func (ck *Clerk) Get(key string) string {
	args := &KV.GetArgs{Key: key}
	id := rand.Intn(len(ck.servers)+10) % len(ck.servers)
	// syh：随机生成id，不太懂为啥取余运算
	for {
		reply, ok := ck.getValue(ck.servers[id], args)
		if ok {
			fmt.Println(id)
			return reply.Value
		} else {
			fmt.Println("can not connect ", ck.servers[id], "or it's not leader")
		}
		id = rand.Intn(len(ck.servers)+10) % len(ck.servers)
	}
}
// syh：将生成的键arg传给ck.servers[id]，若成功，返回键值；失败则生成一个新的随机id，以便下一次循环中选择另一个服务器进行尝试。
// syh：要用到下面定义的getValue方法


func (ck *Clerk) getValue(address string, args *KV.GetArgs) (*KV.GetReply, bool) {
	// Initialize Client
	conn, err := grpc.Dial(address, grpc.WithInsecure()) //,grpc.WithBlock())
	if err != nil {
		log.Printf("did not connect: %v", err)
		return nil, false
	}
	defer conn.Close()
	client := KV.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.Get(ctx, args)
	if err != nil {
		return nil, false
		log.Printf(" getValue could not greet: %v", err)
	}
	return reply, true
}
// syh:向服务器发送"Get"请求，并获取服务器的响应


func (ck *Clerk) Put(key string, value string) bool {
	// You will have to modify this function.
	args := &KV.PutAppendArgs{Key: key, Value: value, Op: "Put", Id: ck.id, Seq: ck.seq}
	id := ck.leaderId
	for {
		//fmt.Println(id)
		reply, ok := ck.putAppendValue(ck.servers[id], args)
		//fmt.Println(ok)

		if ok && reply.IsLeader {
			ck.leaderId = id
			return true
		} else {
			fmt.Println(ok, "can not connect ", ck.servers[id], "or it's not leader")
		}
		id = (id + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Append(key string, value string) bool {
	// You will have to modify this function.
	args := &KV.PutAppendArgs{Key: key, Value: value, Op: "Append", Id: ck.id, Seq: ck.seq}
	id := ck.leaderId
	for {
		reply, ok := ck.putAppendValue(ck.servers[id], args)
		if ok && reply.IsLeader {
			ck.leaderId = id
			return true
		}
		id = (id + 1) % len(ck.servers)
	}
}

func (ck *Clerk) putAppendValue(address string, args *KV.PutAppendArgs) (*KV.PutAppendReply, bool) {
	// Initialize Client
	conn, err := grpc.Dial(address, grpc.WithInsecure()) //,grpc.WithBlock())
	if err != nil {
		return nil, false
		log.Printf(" did not connect: %v", err)
	}
	defer conn.Close()
	client := KV.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.PutAppend(ctx, args)
	if err != nil {
		return nil, false
		log.Printf("  putAppendValue could not greet: %v", err)
	}
	return reply, true
}

var count int32 = 0

// todo: 补全对读取和写入延迟的测试函数，可参考readrequest
func Readrequest(num int, servers[] string) {
	ck := Clerk{}
	ck.servers = make([]string, len(servers))

	for i := 0; i < len(servers); i++ {
		ck.servers[i] = servers[i] + "1"
	}

	for i := 0; i < num; i++ {
		ck.Get("key")
		atomic.AddInt32(&count, 1)
	}
}


// 添加延迟测试函数
func Readrequest(num int, servers []string) {
    ck := Clerk{}
    ck.servers = make([]string, len(servers))

    // 根据服务器列表构建客户端对象
    for i := 0; i < len(servers); i++ {
        ck.servers[i] = servers[i] + "1"
    }

    for i := 0; i < num; i++ {
        start := time.Now() // 记录开始时间
        ck.Get("key") // 执行读取操作
        end := time.Now() // 记录结束时间
        latency := end.Sub(start).Milliseconds() // 计算操作的延迟时间
        fmt.Printf("延迟: %d 毫秒\n", latency) // 打印延迟时间
        atomic.AddInt32(&count, 1) // 增加总请求数计数
    }
}


func main() {
	// 定义服务器地址
	var ser = flag.String("servers", "", "Input Your follower")
	// 定义模式
	var mode = flag.String("mode", "", "Input Your follower")
	// 定义客户端数量
	var cnums = flag.String("cnums", "", "Input Your follower")
	// 定义操作数量
	var onums = flag.String("onums", "", "Input Your follower")
	flag.Parse()
	servers := strings.Split(*ser, ",")
	clientNumm, _ := strconv.Atoi(*cnums)
	optionNumm, _ := strconv.Atoi(*onums)

	if *mode == "testlatency" {
        // 运行延迟测试模式
        fmt.Println("运行延迟测试模式...")
        
        // 检查是否提供了客户端数量和操作数量的参数
        if clientNumm == 0 {
            fmt.Println("##########################################")
            fmt.Println("### 请不要忘记输入 -cnum 的值！   ###")
            fmt.Println("##########################################")
            return
        }
        if optionNumm == 0 {
            fmt.Println("##########################################")
            fmt.Println("### 请不要忘记输入 -onumm 的值！  ###")
            fmt.Println("##########################################")
            return
        }
        
        // 启动指定数量的客户端协程
        for i := 0; i < clientNumm; i++ {
            go Readrequest(optionNumm, servers)
        }
        
        // 定期打印已处理的总请求数
        for {
            time.Sleep(1 * time.Second)
            totalRequests := atomic.LoadInt32(&count)
            fmt.Printf("总请求数: %d\n", totalRequests)
        }
    }

}
