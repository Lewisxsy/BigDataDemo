package edu.hubu1

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

// 订单编号，用户编号，商品编号，消费金额，消费时间
case class Order(oderId:String,userId:String,itemId:String,price:Int,time:Long)
object SourceDemo {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // todo: 2. Source：获取数据源
//    env.readTextFile()
//val sourceDS: DataStream[Any] = env.fromElements("flink", 100, 3.14, true)
//val sourceDS: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
//val sourceDS: DataStream[String] = env.socketTextStream("192.168.139.132", 7777)
//val sourceDS: DataStream[Order] = env.addSource(new OrderSourceFunc)
val sourceDS: DataStream[String] = env.readTextFile("E:\\学习\\大三上\\大数据\\新建文件夹 (2)\\test project\\src\\main\\resources\\result.txt")
//     todo: 3. Transformation：处理数据
    val mapDS: DataStream[Order] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      Order(arr(0),arr(1),arr(2), arr(3).toInt, arr(4).toLong)
    })
      .assignAscendingTimestamps(_.time*1000L)
    // 统计窗口内记录的条数
        val resultDS: DataStream[Int] = mapDS.keyBy(_.itemId)
          .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
          .aggregate(new CountAggFunc)
//    val resultDS: DataStream[Int] = mapDS.keyBy(_.userId)
//      .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
//      .aggregate(new CountAggFunc)
    // todo: 4. Sink：指定结果位置
    resultDS.print()
//    sourceDS.writeAsText("d:\\aaout\\result.txt")
    // todo: 5. 启动执行
    env.execute("source job")
  }
}

//class OrderSourceFunc extends SourceFunction[Order] {
//  private var count:Long=0L
//  private var isRunning:Boolean=true
//  private val random: Random = new Random()
//  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
//    while (isRunning && count<1000){
//      // 订单编号，用户编号，商品编号，消费金额，消费时间
//      val orderId: String = UUID.randomUUID().toString
//      val uid: Int = random.nextInt(6)
//      val userId: String = "user" + uid
//      val i: Int = random.nextInt(10)
//      val itemId: String = "item" + i
//      val price: Int = random.nextInt(1000) + 100
//      val time: Long = System.currentTimeMillis()
//      ctx.collect(orderId,userId,itemId,price,time)
//      TimeUnit.SECONDS.sleep(1)
//    }
//  }
//
//  override def cancel(): Unit = {
//    isRunning=false
//  }
//}
class CountAggFunc extends AggregateFunction[Order,Int,Int] {
  override def createAccumulator(): Int = 0

  override def add(value: Order, accumulator: Int): Int = accumulator+1

  override def getResult(accumulator: Int): Int = accumulator

  override def merge(a: Int, b: Int): Int = a+b
}