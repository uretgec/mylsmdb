# MYLSMDB
Log Structured Merges Tree databases (levelDB, pogreb and nutsdb) based storage unit

Use:
- bbolt: go.etcd.io/bbolt
- sniper: github.com/recoilme/sniper

NOTE:
> If use only boltdb, all key-value data are at in-memory and saves all data to snapshot file for recovery

> If use only sniperdb, all index data are at in-memory and save all key-value data to file (multiple files)
> sniperdb have to use bboltdb index for list, prevlist, exist methods

> You can use both db together without any problems.

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

Bbolt (https://github.com/etcd-io/bbolt)

Sniper (https://github.com/recoilme/sniper)