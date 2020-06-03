package lemon.hbase

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liuyi on 2020/6/2 12:07
  */

/**
  * HBaseContext的方式操作HBase
  *
  * 1>无缝的使用Hbase connection
  *
  * 2>和Kerberos无缝集成
  *
  * 3>通过get或者scan直接生成rdd
  *
  * 4>利用RDD支持hbase的任何组合操作
  *
  * 5>为通用操作提供简单的方法，同时通过API允许不受限制的未知高级操作
  *
  * 6>支持java和scala
  *
  * 7>为spark和 spark streaming提供相似的API
  */
object SparkOnHbase {

  def main(array: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("test-SparkOnHbase")
    val sc = new SparkContext(sparkConf)

    val hbaseConf = HBaseConfiguration.create()

    //有hbase-site.xml 文件的话 以下参数不必

/*
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "fdw6.fengjr.inc,fdw5.fengjr.inc,fdw4.fengjr.inc")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //失败重试时等待时间，随着重试次数越多,重试等待时间越长
    hbaseConf.setInt("hbase.client.pause", 100)
    //失败重试次数
    hbaseConf.setInt("hbase.client.retries.number", 3)
    //该参数表示一次RPC请求的超时时间。如果某次RPC时间超过该值，客户端就会主动关闭socket。改为10秒钟
    hbaseConf.setInt("hbase.rpc.timeout", 3 * 1000)
    //该参数表示HBase客户端发起一次数据操作直至得到响应之间总的超时时间
    hbaseConf.setInt("hbase.client.operation.timeout", 30 * 1000)
    //该参数是表示HBase客户端发起一次scan操作的rpc调用至得到响应之间总的超时时间
    hbaseConf.setInt("hbase.client.scanner.timeout.period", 10 * 1000)

    */

    val hbaseContext = new HBaseContext(sc,hbaseConf)
    val scan = new Scan()
    scan.setCaching(50) //每次从服务器端读取的行数，默认为配置文件中设置的值 减少rpc次数 但是占内存
    //scan.setBatch(20) //你需要的列
    //在查询的时候，按照查询的列数动态设置batch，如果全查，则根据自己所有的表的大小设置一个折中的数值，caching就和分页的值一样就行
    /**
      * 当caching和batch都为1的时候，我们要返回10行具有20列的记录，
      * 就要进行201次RPC，因为每一列都作为一个单独的Result来返回，这样是我们不可以接受的。
      */

    //scan.setStartRow(Bytes.toBytes("start_rowKey"))
    //scan.setStopRow(Bytes.toBytes("end_rowKey"))

    val hbaseTableName = "rt_magpie_risk_channel"

    val hbaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName),scan)
    val rdd =hbaseRDD.take(100)
    hbaseContext.bulk


    var messageList = List[String]()
    var rowkey =""
    var x =0

    for(r <- rdd){
      /**
        * val 的ImmutableBytesWritable类型一般作为RowKey的类型: 但也有时候会把值读出来故有了转化为string一说
        */
      rowkey = Bytes.toString(r._1.get())//其实这个是start rowkey 所有记录都是这个 真正的结果rowkey在cell中就是下面的row
      val result = r._2
      val cells = result.listCells()

      import scala.collection.JavaConversions._
      for(cell:Cell <- cells){

        val row =Bytes.toString(CellUtil.cloneRow(cell))
        val family =Bytes.toString(CellUtil.cloneFamily(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val colum = Bytes.toString(CellUtil.cloneQualifier(cell))

        val message = x+"rowkey:"+rowkey+" row:"+row + " family:"+family+" colum:"+colum+" value:"+value
        messageList.::=(message)
      }
      x=x+1

    }

    val messageRDD = sc.parallelize(messageList)
    messageRDD.coalesce(1,true).saveAsTextFile("hdfs://feng-cluster/user/liuyi/test4.txt")

    /**
      * 最终结果如下
      *
      * 2rowkey:20200602_24_ZLQB row:20200526_24_JQNS family:d colum:channel_name value:借钱能手
        1rowkey:20200602_24_ZLQB row:20200526_24_BEIDOU family:d colum:risk_user_income_count value:325.0
        1rowkey:20200602_24_ZLQB row:20200526_24_BEIDOU family:d colum:channel_name value:51信用卡
        0rowkey:20200602_24_ZLQB row:20200526_24_APP family:d colum:risk_user_income_count value:1788.0
        0rowkey:20200602_24_ZLQB row:20200526_24_APP family:d colum:channel_name value:喜鹊快贷
      */


    /**
      * 这是rdd 每一条的数据格式(ImmutableBytesWritable, Result)  可以发现第一个元素都一样
      *
      * (32 30 32 30 30 36 30 32 5f 32 34 5f 5a 4c 51 42,keyvalues={20200531_24_YQG/d:anti_fraud_fail_user_count/1590924545125/Put/vlen=3/seqid=0, 20200531_24_YQG/d:anti_fraud_succ_user_count/1590925923797/Put/vlen=4/seqid=0, 20200531_24_YQG/d:channel_name/1590939244195/Put/vlen=9/seqid=0, 20200531_24_YQG/d:risk_access_refuse_user_count/1590910743988/Put/vlen=3/seqid=0, 20200531_24_YQG/d:risk_access_succ_user_count/1590925923795/Put/vlen=4/seqid=0, 20200531_24_YQG/d:risk_end_result_fail_user_count/1590925923802/Put/vlen=4/seqid=0, 20200531_24_YQG/d:risk_end_result_succ_user_count/1590915843928/Put/vlen=3/seqid=0, 20200531_24_YQG/d:risk_manucheck_fail_user_count/1590892925047/Put/vlen=3/seqid=0, 20200531_24_YQG/d:risk_model_fail_user_count/1590925923799/Put/vlen=4/seqid=0, 20200531_24_YQG/d:risk_model_succ_user_count/1590915843926/Put/vlen=3/seqid=0, 20200531_24_YQG/d:risk_user_income_count/1590939244195/Put/vlen=4/seqid=0})
      * (32 30 32 30 30 36 30 32 5f 32 34 5f 5a 4c 51 42,keyvalues={20200531_24_JQNS/d:anti_fraud_fail_user_count/1590926463788/Put/vlen=5/seqid=0, 20200531_24_JQNS/d:anti_fraud_succ_user_count/1590926463789/Put/vlen=6/seqid=0, 20200531_24_JQNS/d:channel_name/1590940683763/Put/vlen=12/seqid=0, 20200531_24_JQNS/d:risk_access_refuse_user_count/1590926403942/Put/vlen=5/seqid=0, 20200531_24_JQNS/d:risk_access_succ_user_count/1590926463786/Put/vlen=6/seqid=0, 20200531_24_JQNS/d:risk_end_result_fail_user_count/1590926463794/Put/vlen=6/seqid=0, 20200531_24_JQNS/d:risk_end_result_succ_user_count/1590926224113/Put/vlen=5/seqid=0, 20200531_24_JQNS/d:risk_manucheck_fail_user_count/1590920283977/Put/vlen=4/seqid=0, 20200531_24_JQNS/d:risk_manucheck_succ_user_count/1590921485568/Put/vlen=3/seqid=0, 20200531_24_JQNS/d:risk_model_fail_user_count/1590926463791/Put/vlen=6/seqid=0, 20200531_24_JQNS/d:risk_model_succ_user_count/1590926224110/Put/vlen=5/seqid=0, 20200531_24_JQNS/d:risk_user_income_count/1590940683763/Put/vlen=6/seqid=0})
      * (32 30 32 30 30 36 30 32 5f 32 34 5f 5a 4c 51 42,keyvalues={20200531_24_RONG/d:anti_fraud_fail_user_count/1590917944970/Put/vlen=3/seqid=0, 20200531_24_RONG/d:anti_fraud_succ_user_count/1590919204847/Put/vlen=4/seqid=0, 20200531_24_RONG/d:channel_name/1590939424146/Put/vlen=9/seqid=0, 20200531_24_RONG/d:risk_access_refuse_user_count/1590907744118/Put/vlen=3/seqid=0, 20200531_24_RONG/d:risk_access_succ_user_count/1590919204845/Put/vlen=4/seqid=0, 20200531_24_RONG/d:risk_end_result_fail_user_count/1590919204853/Put/vlen=4/seqid=0, 20200531_24_RONG/d:risk_manucheck_fail_user_count/1590919204850/Put/vlen=3/seqid=0, 20200531_24_RONG/d:risk_model_fail_user_count/1590915004365/Put/vlen=4/seqid=0, 20200531_24_RONG/d:risk_model_succ_user_count/1590919204849/Put/vlen=3/seqid=0, 20200531_24_RONG/d:risk_user_income_count/1590939424146/Put/vlen=4/seqid=0})
      * (32 30 32 30 30 36 30 32 5f 32 34 5f 5a 4c 51 42,keyvalues={20200531_24_RONG360/d:anti_fraud_fail_user_count/1590925565401/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:anti_fraud_succ_user_count/1590926403945/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:channel_name/1590940804816/Put/vlen=6/seqid=0, 20200531_24_RONG360/d:risk_access_refuse_user_count/1590926343776/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:risk_access_succ_user_count/1590926403942/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:risk_end_result_fail_user_count/1590926403949/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:risk_end_result_succ_user_count/1590925864138/Put/vlen=4/seqid=0, 20200531_24_RONG360/d:risk_manucheck_fail_user_count/1590913384742/Put/vlen=4/seqid=0, 20200531_24_RONG360/d:risk_manucheck_succ_user_count/1590919863844/Put/vlen=3/seqid=0, 20200531_24_RONG360/d:risk_model_fail_user_count/1590926403947/Put/vlen=5/seqid=0, 20200531_24_RONG360/d:risk_model_succ_user_count/1590925864136/Put/vlen=4/seqid=0, 20200531_24_RONG360/d:risk_user_income_count/1590940804816/Put/vlen=6/seqid=0})
      * (32 30 32 30 30 36 30 32 5f 32 34 5f 5a 4c 51 42,keyvalues={20200531_24_RONGSHU/d:anti_fraud_fail_user_count/1590907024781/Put/vlen=5/seqid=0, 20200531_24_RONGSHU/d:anti_fraud_succ_user_count/1590921184199/Put/vlen=6/seqid=0, 20200531_24_RONGSHU/d:channel_name/1590923645170/Put/vlen=6/seqid=0, 20200531_24_RONGSHU/d:risk_access_refuse_user_count/1590912064340/Put/vlen=5/seqid=0, 20200531_24_RONGSHU/d:risk_access_succ_user_count/1590921184197/Put/vlen=6/seqid=0, 20200531_24_RONGSHU/d:risk_end_result_fail_user_count/1590920224412/Put/vlen=6/seqid=0, 20200531_24_RONGSHU/d:risk_end_result_succ_user_count/1590921184206/Put/vlen=5/seqid=0, 20200531_24_RONGSHU/d:risk_manucheck_fail_user_count/1590920224410/Put/vlen=4/seqid=0, 20200531_24_RONGSHU/d:risk_manucheck_succ_user_count/1590921184204/Put/vlen=4/seqid=0, 20200531_24_RONGSHU/d:risk_model_fail_user_count/1590908103788/Put/vlen=6/seqid=0, 20200531_24_RONGSHU/d:risk_model_succ_user_count/1590921184202/Put/vlen=5/seqid=0, 20200531_24_RONGSHU/d:risk_user_income_count/1590923645170/Put/vlen=6/seqid=0})
      */

  }

}
