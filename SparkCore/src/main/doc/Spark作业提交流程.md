
##### 1、Spark-submit脚本
    脚本路径：
        spark-2.2.0/bin/spark-submit
    脚本内容：
        if [ -z "${SPARK_HOME}" ]; then
          source "$(dirname "$0")"/find-spark-home
        fi
        # disable randomized hash for string in Python 3.3+
        export PYTHONHASHSEED=0
        exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"

    脚本最后调用exec执行 "${SPARK_HOME}"/bin/spark-class，调用的class为：org.apache.spark.deploy.SparkSubmit，后面的 "$@" 是脚本执行的所有参数。
    通过 spark-class 脚本，最终执行的命令中，指定了程序的入口为org.apache.spark.deploy.SparkSubmit。

##### 2、org.apache.spark.deploy.SparkSubmit类
    类的main方法：
        override def main(args: Array[String]): Unit = {
            // 获取参数解析列表
            val appArgs = new SparkSubmitArguments(args)
            // 用于确认是否打印一些JVM信息，默认为false。
            if (appArgs.verbose) {
                // scalastyle:off println
                printStream.println(appArgs)
                // scalastyle:on println
            }
            // 提交作业的行为方式
            appArgs.action match {
                case SparkSubmitAction.SUBMIT => submit(appArgs) // 提交spark作业
                case SparkSubmitAction.KILL => kill(appArgs)
                case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
            }
        }    
    根据解析后的参数中的 action 进行模式匹配，submit 操作，则调用 submit 方法。

##### 3、submit方法
    


