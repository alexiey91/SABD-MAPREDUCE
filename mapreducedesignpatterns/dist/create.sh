docker network create --driver bridge hadoop_network

docker run -t -i -p 50075:50075 -p 50061:50060 -d --network=hadoop_network --name=slave1 matnar/hadoop
docker run -t -i -p 50076:50075 -p 50062:50060 -d --network=hadoop_network --name=slave2 matnar/hadoop
docker run -t -i -p 50077:50075 -p 50063:50060 -d --network=hadoop_network --name=slave3 matnar/hadoop
docker run -t -i -p 50070:50070 -p 50060:50060 -p 50030:50030 -p 8088:8088 -p 19888:19888 --network=hadoop_network --name=master -v $PWD/hddata:/data matnar/hadoop

