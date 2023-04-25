package edu.hubu

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // todo: 2. Source：获取数据源
    val sourceDS: DataSet[String] = env.readTextFile("E:\\学习\\大三上\\大数据\\新建文件夹 (2)\\test project\\src\\main\\resources\\word.txt")

    // todo: 3. Transformation：处理数据
    val resultDS: AggregateDataSet[(String, Int)] = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    // todo: 4. Sink：指定结果位置
    resultDS.print()

    // todo: 5. 启动执行
  }
}
