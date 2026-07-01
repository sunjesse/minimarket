## minimarket

fun project to learn more about websockets, tokio, rayon (although we opt out of using this after implementation, realizing its the wrong tool - we instead just shard each order among a threadpool instead), and market making! :)

high level overview:
![](https://github.com/sunjesse/minimarket/blob/main/assets/diagram.png)

currently: 
- the order matching algorithm is using plain ol' FIFO
- we allow clients to run the following operations: market buy/sells, limit buy/sells.
- single process server on my 2022 M2 Macbook Air sustains a throughput of 120k orders/s (dev build) - on a release build, a single client can submit up to 1.1M orders per second, but the matcher is not able to sustain this level of throughput on this current setup with this current code, we have lots of backpressure - matcher caps out at about 360k / S. it'd be interesting to see how much we can improve!

### setup (on Mac)
We use k3s for running on our home-server, but that requires a Linux machine. for local dev on a mac for example, we instead use k3d, which runs k3s inside docker.

cmds:
```
docker build -t mm:latest . # build the image
k3d cluster create minimarket -p "30080:30080@server:0" # create the cluster, forwarding node-port 30080 to my mac's localhost

k3d image import mm:latest -c minimarket # k3d has its own containerd image store that is separate from docker, hence we must import our image into k3d

helm install minimarket charts/mm-exchange/ # install the helm chart
```

now, we can use the usual kubectl cmds to check on the status. we deploy the server as a statefulset as each server needs to persist state to a persistent volume (recall, we store market prices and bids/asks in-memory, with periodic snapshots to disk).

now, on the same machine (for local dev, eventually i want cross machine), connect the client:

```
cargo run --bin client -- ws://127.0.0.1:30080 --auto
```

### setup (on Linux) machine
for running on my linux machine:

```
docker build -t mm:latest .
docker save mm:latest -o mm.tar
sudo k3s ctr images import mm.tar # again, like before, we import our comtainer into k3s managed containerd

helm install minimarket charts/mm-exchange/
```


### iterating locally

```
docker build -t mm:latest .
docker save mm:latest -o mm.tar && sudo k3s ctr images import mm.tar
kubectl rollout restart statefulset minimarket-server
```
