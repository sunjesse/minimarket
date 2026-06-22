## minimarket

fun project to learn more about websockets, tokio, rayon (although we opt out of using this after implementation, realizing its the wrong tool - we instead just shard each order among a threadpool instead), and market making! :)

high level overview:
![](https://github.com/sunjesse/minimarket/blob/main/assets/diagram.png)

currently: 
- the order matching algorithm is using plain ol' FIFO
- we allow clients to run the following operations: market buy/sells, limit buy/sells.
- single process server on my 2022 M2 Macbook Air sustains a throughput of 120k orders/s (dev build) - on a release build, a single client can submit up to 1.1M orders per second, but the matcher is not able to sustain this level of throughput on this current setup with this current code, we have lots of backpressure - matcher caps out at about 360k / S. it'd be interesting to see how much we can improve!
