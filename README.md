# kafka-poc

Produce and consume Kafka messages

## Testing

There is a live version of the `fast-data-dev` image at the following location:

https://fast-data-dev.demo.landoop.com/

The organization has changed its name from Landoop to LensesIO. The images is here:

https://github.com/lensesio/fast-data-dev

### Docker

```
docker-compose up -d
```

### Coyote Tests

http://localhost:3030/coyote-tests/

```
$ curl -vs --stderr - "http://localhost:8082/topics"
*   Trying ::1...
* TCP_NODELAY set
* Connected to localhost (::1) port 8082 (#0)
> GET /topics HTTP/1.1
> Host: localhost:8082
> User-Agent: curl/7.63.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Jan 2020 18:20:36 GMT
< Content-Type: application/vnd.kafka.v1+json
< Vary: Accept-Encoding, User-Agent
< Content-Length: 260
< Server: Jetty(9.4.14.v20181114)
<
["_schemas","backblaze_smart","connect-configs","connect-offsets","connect-statuses","coyote-test-avro","coyote-test-binary","coyote-test-json","logs_broker","nyc_yellow_taxi_trip_data","sea_vessel_position_reports","telecom_italia_data","telecom_italia_grid"]* Connection #0 to host localhost left intact
```

## References

1. [Getting started with kafka](https://success.docker.com/article/getting-started-with-kafka)
