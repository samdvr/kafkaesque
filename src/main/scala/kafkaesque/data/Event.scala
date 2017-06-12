package kafkaesque.data

trait Event {
  val message:String
  val topic: String
}
