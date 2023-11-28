# Store tests

## How to test store with MySQL?

1. Create a database in your MySQL server.
2. Run the following command with two environment variables set:

```go
DRIVER=mysql DSN=root@/memos_test go test -v ./test/store/...
```

- `DRIVER` should be set to `mysql`.
- `DSN` should be set to the DSN of your MySQL server.

## How to test store with Postgres?

1. Create a database in your Postgres server.
2. Run the following command with two environment variables set:

```go
DRIVER=postgres DSN=$postgres_dns go test -v ./test/store/...
```

- `DRIVER` should be set to `postgres`.
- `DSN` should be set to the DSN of your postgres server.

