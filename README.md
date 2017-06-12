# Kafkaesque

A simple Kafka Producer

## Usage

In order to publish an event create an event extending kafkaesque.data.Event trait which has the following values:
```topic:String, message: String```

Then import Kafkaesque._

Now any ```Seq[Event]``` will have publish that produces the event in the Sequence.

## Configuration
Kafkaesque by default produces messages to ```localhost:3899``` as the broker host. If you'd like to change this behavior you can define values for kafkaesque.brokers in your application.conf file.

### Example:

```scala
import kafkaesque.data.Event

case class Deposit(message: String, topic: String) extends Event
object Main extends App{
  import Kafkaesque._
  override def main(args: Array[String]): Unit = {
    Seq(Deposit("Hello From Kafkaesque", "events")).publish
  }
}
```
