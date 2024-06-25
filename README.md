# Terraform Provider Cockroach Extra

_Provides additional resources for CockroachDB hosted on Cockroach Cloud_

## Introduction
The official CockroachDB terraform provider provides most of the resources needed to get a cluster up and running, but does not provide much help getting it setup afterwards.
This provider aims to cover some of the common configuration requirements.

## Project State
This project is under active development and changes can be dramatic between versions.
The provider is currently in use here at nRFCloud and features that are needed for our use cases will be prioritized.

Documentation is currently incomplete.


### Resources
- `cockroach-extra_sql_user` - Manage an unprivileged SQL user
- `cockroach-extra_sql_role` - Manage a SQL role that can be granted to users
- `cockroach-extra_sql_grant` - Grant a role to a user
- `cockroach-extra_external_connection` - Manage an external connection resource that can be used for changefeeds and backups
- `cockroach-extra_cluster_setting` - Manage the value of a cluster-wide setting
- `cockroach-extra_changefeed` - Manage a changefeed connected to an external destination
- `cockroach-extra_backup_schedule` - Manage a backup schedule
- `cockroach-extra_migration` - Manage running migrations using golang-migrate
- `cockroach-extra_persistent_cursor` - Manage a 'persistent cursor' resource that allows maintaining the resolved timestamp of a changefeed across replacements

## Requirements

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [Go](https://golang.org/doc/install) >= 1.19

## Building The Provider

1. Clone the repository
1. Enter the repository directory
1. Build the provider using the Go `install` command:

```shell
go install
```

## Adding Dependencies

This provider uses [Go modules](https://github.com/golang/go/wiki/Modules).
Please see the Go documentation for the most up to date information about using Go modules.

To add a new dependency `github.com/author/dependency` to your Terraform provider:

```shell
go get github.com/author/dependency
go mod tidy
```

Then commit the changes to `go.mod` and `go.sum`.

## Using the provider

Fill this in for each provider

## Developing the Provider

If you wish to work on the provider, you'll first need [Go](http://www.golang.org) installed on your machine (see [Requirements](#requirements) above).

To compile the provider, run `go install`. This will build the provider and put the provider binary in the `$GOPATH/bin` directory.

To generate or update documentation, run `go generate`.

In order to run the full suite of Acceptance tests, run `make testacc`.

*Note:* Acceptance tests create real resources, and often cost money to run.

```shell
make testacc
```
