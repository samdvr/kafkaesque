# Kafkaesque

A simple Kafka Producer

## Usage

Let's say we have defined a case class as follows:
```Scala
case class Person(name: String, age: Int)
```

after importing ```Kafkaesque._```

Any instance of our case class has publish which produces the event to Kafka

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
