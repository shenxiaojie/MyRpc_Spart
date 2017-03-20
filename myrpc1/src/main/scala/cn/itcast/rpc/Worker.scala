package cn.itcast.rpc

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory
import  scala.concurrent.duration._

/**
  * Created by xiaoquan on 2017/3/19.
  */
//注意:继承的是Akka的Actor
//Woker要接收消息，还要发送消息
class Worker( val masterhost:String,val masterport:Int,val memory:Int,val cores:Int) extends  Actor{
  var master:ActorSelection=_
  var workId=UUID.randomUUID().toString
  //10秒
  val heart_interval=10000
  override def receive: Receive =
  {
     //接收到master的回馈信息
    case RegisteredWoker(masterUrl)=>
      {
        println(masterUrl)
        //启动定时器发送心跳。10秒钟向Master发一个心跳
        //先给自己发，然后再发给Master
        import context.dispatcher
        context.system.scheduler.schedule(0 millis,heart_interval millis,self,SendHeartbeat)



      }


    case SendHeartbeat=>
      {

        println("send herrtbeat to master")

        master!Heartbeat(workId)

      }



  }
//这个方法的运行时间在构造器后面，receive方法之前被调用
  //这个方法里面可以建立与Mast的连接。就是查找某个具体的Actor
  override def preStart(): Unit =
  {
    //MasterSystem是Msterer那边的AcctorSystem老大。Master是创建的Actor的名字。最后通过IP和端口号和Actor的名字（Master）找到了Master。使用TCP协议
   //跟master建立连接
     master=context.actorSelection(s"akka.tcp://MasterSystem@$masterhost:$masterport/user/Master")
    //发送一个Worker向Master的注册类,把自己的内存，内核数都告诉给Master
    master!RegisterWorker(workId,memory,cores)



  }
}

object  Worker{


  def main(args: Array[String]): Unit = {
    val host=args(0)
    val port=args(1).toInt
    val masterHost=args(2)
    val masterPort=args(3).toInt
    val memory=args(4).toInt
    val cores=args(5).toInt
    val configstr=
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config=ConfigFactory.parseString(configstr)

    //ActorSystem是老大，负责监控下面的Actor,并且ActorSystem是单例的,我们去看源码发现就是object的单例
    val actorSystem=ActorSystem("WorkerSystem",config)
    val worker=actorSystem.actorOf(Props(new Worker(masterHost,masterPort,memory,cores)),"worker")
    actorSystem.awaitTermination()
  }
}