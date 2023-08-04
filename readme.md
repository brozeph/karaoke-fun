# Karaoke-Fun

## Prerequisites

* Go 1.20+

## Setup

```bash
git clone https://github.com/brozeph/karaoke-fun.git
cd karaoke-fun
go get all
```

## Run the import

### Start a MongoDB instance for the karaoke database

If using Docker, the following may be useful (be sure to adjust the port if necessary or when running multiple mongo instances simultaneously):

```bash
mkdir -p .mongo
docker run -d -p 27017:27017 --name karaoke-db -v $PWD/.mongo:/data/db mongo
```

### Execute the import command

```bash
go run cmd/main.go
```