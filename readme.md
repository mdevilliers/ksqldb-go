docker-compoe up

docker run --network ksqldb-go_default --rm --name datagen-users \
    confluentinc/ksql-examples:latest \
    ksql-datagen \
        bootstrap-server=kafka:39092 \
        quickstart=users \
        format=json \
        topic=users \
        maxInterval=100

docker run --network ksqldb-go_default --rm --name datagen-pageviews \
    confluentinc/ksql-examples:latest \
    ksql-datagen \
        bootstrap-server=kafka:39092 \
        quickstart=pageviews \
        format=delimited \
        topic=pageviews \
        maxInterval=500

go build && ./ksqldb-go
