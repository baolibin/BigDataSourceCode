
### 0、作业提交流程
    1）、编写好自己的SparkCore代码，编译成jar包。上传到Spark环境服务器。
    2）、使用spark-submit脚本提交任务运行。
    3）、运行SparkSubmit类的main方法，通过反射的方式创建编写的主类的实例对象，Driver运行在SparkSubmit进程中。
    4）、根据提交作业的模式启动Driver，初始化创建SparkContext，此时会创建DAGScheduler和TaskScheduler。
    5）、【Driver请求Cluster Manager分配资源启动Executor线程。Executor启动后反向注册到Driver中，Driver会分发jar包到各个Executor上。】
    6）、在构建TaskScheduler的同时，会创建DriverActor（接受executor的反向注册，将任务提交给executor）和ClientActor（向master注册用户提交的任务）。
    7）、当ClientActor启动后，会将用户提交的任务和相关的参数封装到ApplicationDescription对象中，然后提交给master进行任务的注册。
    8）、当master接受到clientActor提交的任务请求时，会将请求参数进行解析，并封装成Application，然后将其持久化，然后将其加入到任务队列waitingApps中。
    9）、当轮到我们提交的任务运行时，就开始调用 schedule()，进行任务资源的调度，master将调度好的资源封装到launchExecutor中发送给指定的 worker。
    10）、worker接受到Master发送来的launchExecutor时，会将其解压并封装到ExecutorRunner中，然后调用这个对象的start(), 启动Executor。
    11）、Executor启动后会向DriverActor进行反向注册，driverActor会发送注册成功的消息给Executor。
    12）、Executor接受到DriverActor注册成功的消息后会创建一个线程池，用于执行 DriverActor发送过来的task任务。
    13）、当属于这个任务的所有的Executor启动并反向注册成功后，就意味着运行这个任务的环境已经准备好了，driver会结束SparkContext对象的初始化，也就意味着new SparkContext这句代码运行完成。
    14)、当初始化sc成功后，driver端就会继续运行我们编写的代码，然后开始创建初始的RDD，然后进行一系列转换操作，当遇到一个action算子时，也就意味着触发了一个job。
    15）、driver会将这个job提交给DAGScheduler，DAGScheduler将接受到的job，从最后一个算子向前推导，将DAG依据宽依赖划分成一个一个的stage，然后将stage封装成taskSet，并将taskSet中的task提交给DriverActor。
    16）、DriverActor接受到DAGScheduler发送过来的task，会拿到一个序列化器，对task进行序列化，然后将序列化好的task封装到launchTask中，然后将launchTask发送给指定的Executor。
    17）、Executor接受到了DriverActor发送过来的launchTask时，会拿到一个反序列化器，对launchTask进行反序列化，封装到TaskRunner中，然后从Executor这个线程池中获取一个线程，将反序列化好的任务中的算子作用在RDD对应的分区上。

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
    


