## minimarket

fun project to learn more about websockets, tokio, rayon (although we opt out of using this after implementation, realizing its the wrong tool - we instead just shard each order among a threadpool instead), and market making! :)

high level overview:
![](https://github.com/sunjesse/minimarket/blob/main/assets/diagram.png)

currently: 
- the order matching algorithm is using plain ol' FIFO
- we allow clients to run the following operations: market buy/sells, limit buy/sells.
- single process server on my 2022 M2 Macbook Air sustains a throughput of 123k orders/s

next up:
- try implementing other matching algos: [pro-rata, lead market maker algo, variants of fifo](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457218479/Supported+Matching+Algorithms)
- efficiently model implied volatility and other statistics about each security.
- profiling
