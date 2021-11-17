# Connection configuration

Shared configuration for the connection pool is located under `akka.persistence.r2dbc.connection-factory`.
You have to set at least:

Postgres:
: @@snip [application.conf](/docs/src/test/resources/application-postgres.conf) { #connection-settings }

Yugabyte:
: @@snip [application.conf](/docs/src/test/resources/application-yugabyte.conf) { #connection-settings }

## Reference configuration 

The following can be overridden in your `application.conf` for the connection settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#connection-settings}
