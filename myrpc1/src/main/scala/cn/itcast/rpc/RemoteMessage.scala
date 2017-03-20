package cn.itcast.rpc

/**
  * Created by xiaoquan on 2017/3/20.
  */
   trait RemoteMessage extends Serializable

  //worker->Master注册的class。
  case class RegisterWorker(id:String,memory:Int,cores:Int) extends  RemoteMessage

  //Master->worker的回复消息.告诉Worker，已经注册成功了
   case class RegisteredWoker(masterUrl:String)extends  RemoteMessage

   //Worker->Worker。自己给自己发送消息，在同一个进程内，所以不需要实现序列化
    case object  SendHeartbeat
  //Worker->self
   case class Heartbeat(id:String)extends  RemoteMessage
   //Master->Master
  case object checkTimeWorkTime

