package cn.itcast.rpc

import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import  scala.concurrent.duration._
/**
  * Created by xiaoquan on 2017/3/19.
  */
//注意:继承的是Akka的Actor
class Master(val host:String,val port:Int) extends  Actor{

  //workerId->WorkerInf引用的
  val idToWorker=new mutable.HashMap[String,Workinfo]()

  //存放Workinfo的一个HashSet。
  val workers=new mutable.HashSet[Workinfo]()
  //超时检测的间隔
  val check_interval=15000
 //主构造器会调用这个方法
  println("constructor invoked")

  //这个方法在构造器后面，receive方法之前被调用
  override def preStart(): Unit =
  {
    println("preStart invoked")
    //定期检测workes是否还活着
    import context.dispatcher
     context.system.scheduler.schedule(0 millis,check_interval millis,self,checkTimeWorkTime)


  }

  override def receive: Receive =
  {
    //Worker发送注册
    case RegisterWorker(id,memory,cores)=> {


      //判断这个worker是不是已经注册过了，如果已经注册过了，那就忽略不计
       //如果没有注册过，那就把worker加入到idToWorker
        if(!idToWorker.contains(id))
          {
            /**
              * 把Worker的信息封装起来保存到内存中
              * 但是保存到内存中是不安全的。
              * 其实还应该保存到zk中
              */
            val workinfo=new Workinfo(id,memory,cores)
            //按照workid和wordinfo的形式保存到HashMap中
            idToWorker(id)=workinfo
            //把work加入到workers列表中
            workers+=workinfo
            //master把worker的消息保存起来之后，给worker一个回复。
            sender()!RegisteredWoker(s"akka.tcp://MasterSystem@$host:$port/user/Master")

          }





      }
    case "hello"=>
      {

        println("hello")
      }

    case Heartbeat(id)=>
      {
        if(idToWorker.contains(id))
          {
             val workinfo=idToWorker(id)
             val currenTime=System.currentTimeMillis()
             workinfo.lastHeartbeatTime=currenTime
          }


      }
    case checkTimeWorkTime=>
      {

        //当前时间
        val currentTime=System.currentTimeMillis();

      //把超时worker干掉
        val toRemove=workers.filter(x =>currentTime - x.lastHeartbeatTime > check_interval)
      for(w<-toRemove)
     {
      workers-=w
      idToWorker-=w.id

     }
        println(workers.size)

      }



  }
}

object  Master
{

  def main(args: Array[String]): Unit = {


    val host=args(0)
    val port=args(1).toInt
    val configstr=
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config=ConfigFactory.parseString(configstr)

    //ActorSystem是老大，负责监控下面的Actor,并且ActorSystem是单例的,我们去看源码发现就是object的单例
    val actorSystem=ActorSystem("MasterSystem",config)
    //创建Actor,并且取个名字叫做"Master",Worker那边会通过这个名字来找到这个Actor
   val master=actorSystem.actorOf(Props(new Master(host,port)),"Master")

     actorSystem.awaitTermination()


  }

}