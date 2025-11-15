## minimarket

fun project to learn more about websockets, tokio, rayon, and market making! :)

high level overview:
![](https://github.com/sunjesse/minimarket/blob/main/assets/diagram.png)

currently: 
- the order matching algorithm is using plain ol' FIFO
- we allow clients to run the following operations: market buy/sells, limit buy/sells.

next up:
- try implementing other matching algos: [pro-rata, lead market maker algo, variants of fifo](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457218479/Supported+Matching+Algorithms)
- currently there is no concept of "bank balance" for each client, this will be interesting to implement soon.
- efficiently model implied volatility and other statistics about each security.
- profiling
