# Redistream
Redistream is a wrapper on redis stream processing commands. It provides higher level api to work with streams.

## Installation
```go get github.com/morilog/redistream```

## Usage
Visit [examples](https://github.com/morilog/redistream/tree/main/examples) for full example of usage

## Features
- Simple Producer (redis itself it simple too)
- Handle messages with golang native channels
- Handle errors asynchronously
- re-Claim and handle pending messages continuously
- Preserve message ordering by using single consumer in consumer group

## License
MIT
