# DataPlatHW
DataPlatHW



```bash


```


```bash

docker pull cassandra:latest
docker run --volume=/var/lib/cassandra --restart=no -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160 --name hw-cass -d cassandra:latest
docker exec -it hw-cass cqlsh
```

data:
https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/data