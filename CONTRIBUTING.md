# Contributing

## Context

This tool uses postgres wire protocol v3 for communicating with clients.
To learn more about the protocol kindly read the following

- https://www.postgresql.org/docs/current/protocol.html
- https://github.com/jackc/pgproto3

We use [pgproto3](https://github.com/jackc/pgproto3) to talk in this protocol.

## Setting up the code in local

- Fork and clone this repo
- Ensure you have the latest version of [golang](https://go.dev/) and [make](https://www.gnu.org/software/make/)
  installed.
- To build the project, run command `make build`
- To run the test suite, run command `make test`
