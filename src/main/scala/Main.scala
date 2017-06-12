import kafkaesque.data.Event

object Main extends App{
  import Kafkaesque._

  override def main(args: Array[String]): Unit = {
    case class Deposit(message: String, topic: String) extends Event
    Seq(Deposit("Hello From Kafkaesque", "events")).publish
  }



}
