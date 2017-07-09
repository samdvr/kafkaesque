# Kafkaesque

A simple Kafka Producer

## Usage

In order to publish an event create a case class
```case class Person(name: String, age: Int)```

Then import Kafkaesque._

Now any ```Person``` instance will have publish that produces the event.

```Scala
import Kafkaesque._
Person("Sherlock", 43).publish("SomeTopic")
```
This also works for Seq[Person]

```Scala
import Kafkaesque._
Seq(Person("Sherlock", 43),Person("John", 46)).publish("SomeTopic")
```


## Configuration
Kafkaesque by default produces messages to ```localhost:3899``` as the broker host. If you'd like to change this behavior you can define values for ```kafkaesque.brokers``` in your ```application.conf``` file.
