# Kafkaesque

Sample kafka producer

## Usage

In order to publish an event create an event extending kafkaesque.data.Event trait which has the following values:
topic:String, message: String

Then import Kafkaesque._

Now Seq[Event] will have publish that defaults to localhost:3899
and invoking it will produce the event.

###Example:

```
import kafkaesque.data.Event

object Main extends App{
  import Kafkaesque._

  override def main(args: Array[String]): Unit = {
    case class Deposit(message: String, topic: String) extends Event
    Seq(Deposit("Hello From Kafkaesque", "events")).publish
  }



}


```
