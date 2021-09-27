# Contributing 

Please feel free to contribute to Akka Persistence R2DBC and the documentation by reporting issues you identify, or by suggesting changes to the code. 
Please refer to our [contributing instructions](https://github.com/akka/akka/blob/main/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).

## Running the tests

The tests expect a locally running database.

It can be started with the docker-comopse file in the docker folder:

@@snip [docker-compose.yml](/docker/docker-compose-postgres.yml)

```
docker-compose -f docker/docker-compose-postgres.yml up
```
