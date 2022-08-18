# MYLSMDB
Log Structured Merges Tree databases (leveldb, pogreb and nutsdb) based storage unit

Use:
- leveldb https://github.com/syndtr/goleveldb
- pogreb https://github.com/akrylysov/pogreb
- nutsdb https://github.com/nutsdb/nutsdb

NOTE:
> ...

## Methods
```
	CloseStore()
	SyncStore()

	Set(bucketName []byte, k []byte, data []byte) ([]byte, error)
	Get(bucketName []byte, k []byte) ([]byte, error)
	MGet(bucketName []byte, keys ...[]byte) (interface{}, error)
	List(bucketName []byte, cursor []byte, perpage int) ([]string, error)
	PrevList(bucketName []byte, cursor []byte, perpage int) ([]string, error)
	Delete(bucketName []byte, k []byte) error

	KeyExist(bucketName []byte, k []byte) (bool, error)
	ValueExist(bucketName []byte, v []byte) (bool, error)

	HasBucket(bucketName []byte) bool
	StatsBucket(bucketName []byte) int
	ListBucket(bucketName []byte) int
	DeleteBucket(bucketName []byte) int

	Backup(path, filename string) error
	Restore(path, filename string) error
```

## Install

```
go get  github.com/uretgec/mylsmdb
```

## TODO
- Add new examples

## Links

Leveldb (https://github.com/syndtr/goleveldb)

Pogreb (https://github.com/akrylysov/pogreb)

Nutsdb (https://github.com/nutsdb/nutsdb)