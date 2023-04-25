package edu.hubu

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

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
val sourceDS: DataStream[Order] = env.addSource(new OrderSourceFunc)
//     todo: 3. Transformation：处理数据
    // todo: 4. Sink：指定结果位置
    sourceDS.print()
    sourceDS.writeAsText("d:\\aaout\\result.txt")
    // todo: 5. 启动执行
    env.execute("source job")
  }
}

class OrderSourceFunc extends SourceFunction[Order] {
  private var count:Long=0L
  private var isRunning:Boolean=true
  private val random: Random = new Random()
  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    while (isRunning && count<1000){
      // 订单编号，用户编号，商品编号，消费金额，消费时间
      val orderId: String = UUID.randomUUID().toString
      val uid: Int = random.nextInt(6)
      val userId: String = "user" + uid
      val i: Int = random.nextInt(10)
      val itemId: String = "item" + i
      val price: Int = random.nextInt(1000) + 100
      val time: Long = System.currentTimeMillis()
      ctx.collect(Order(orderId,userId,itemId,price,time))
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    isRunning=false
  }
}
