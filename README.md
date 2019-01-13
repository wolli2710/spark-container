# spark-container


## Build docker container
docker build -f Dockerfile -t pyspark .

## RUN docker container in interactive mode and work on the linux of the container
docker run --rm -v $PWD:/data -it pyspark /bin/bash

## RUN tests in container and get return code
docker run --rm -v $PWD:/data pyspark
