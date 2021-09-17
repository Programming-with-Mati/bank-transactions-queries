# Bank Transactions: Stateful Processing in Kafka Streams

The purpose of this tutorial is for us to learn about **stateful processing** in Kafka Streams.
The youtube video associated with this tutorial can be found here: [YouTube Video](https://youtube.com/asdafsfas). To follow this tutorial, clone this repository and switch to the branch `tutorial_start`.

```shell
git clone https://github.com/Programming-with-Mati/bank-transactions.git
```
Once the repository is cloned run:
```shell
git checkout tutorial_start
```
To run a working copy of the code, go to [Running the project](#running-the-project)

## Our Application
Mati Bank Ltd. is a bank that holds its customer's bank balances in a Kafka Streams application. We are going to develop a topology that will process bank transactions and will stream updates of every new balance for a given user. The application will also stream all the rejected transactions into a different topic for error handling purposes.

## The Data Model
![DataModel](docs/dataModel.jpeg)

### Bank Transaction
The [`BankTransaction`](src/main/java/com/github/programmingwithmati/model/BankTransaction.java) will hold the data regarding the transactions that will be processed. It will hold an amount that can be either positive or negative, representing credits or debits, respectively.
### Bank Balance
The [`BankBalance`](src/main/java/com/github/programmingwithmati/model/BankBalance.java) class will store the account balance for the user, and it will also hold information about the latest transaction.

## The Topology
![DataModel](docs/Topology.jpeg)
In the topology, we are going to:
1. Stream the topic `bank-transactions` where the key is the `balanceId`
2. `groupByKey the stream
3. Use the `aggregate` operation to aggregate the all the transactions that have the same key into a single account balance. The balances will be stored in a State Store. This operation will return a `KTable`
4. Transform the `KTable` into a `KStream`. This means that any change occurring in the table will be streamed as an event
5. Stream the events into the `bank-balances` topic
6. `map` the `BankBalance` into `BankTransaction` by keeping the last transaction
7. `filter` the transactions to keep only the `REJECTED` transactions
8. `stream` the rejected transactions into the `rejected-transactions` topic

## The code
Given we have done several tutorials on Kafka Streams, I'm providing a stub of the project with a lot of the classes implemented.

The class [BankBalanceApp](src/main/java/com/github/programmingwithmati/BankBalanceApp.java) is the entry point of our application, were we create our `KafkaStreams` application.

The [model](src/main/java/com/github/programmingwithmati/model) package contains the model classes with all its members. We have a method called `process` in `BankBalance` which we will impement as part of this tutorial.

The class [BankBalanceTopology](src/main/java/com/github/programmingwithmati/topology/BankBalanceTopology.java) has an empty method `buildTopology()` which we will also implement.

There is a [BankTransactionProducer](src/main/java/com/github/programmingwithmati/BankTransactionProducer.java) which we will use to run some tests manually.

Finally, there is a test class called [BankBalanceTopologyTest](src/test/java/com/github/programmingwithmati/topology/BankBalanceTopologyTest.java), with two tests already implemented. We will have to make these tests pass as part of our tutorial.

## Running the project

### Starting Kafka
First, we need to start Kafka. For that we have a [docker-compose.yml file](docker-compose.yml) that will create the necessary resources for us. It will start a Zookeeper instance and a Kafka broker. It will also create the necessary topics using the script found in the [create-topics.sh](./scripts/create-topics.sh) file.
```shell
docker compose -f ./docker-compose.yml up
```
### Building and starting the application
```shell
./mvnw compile exec:java -Dexec.mainClass="com.github.programmingwithmati.kafka.streams.wordcount.VoiceCommandParserApp"
```
