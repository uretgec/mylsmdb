package leveldbstorage

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/uretgec/mylsmdb/storage"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Store struct {
	db         *leveldb.DB
	bucketList []string
	readOnly   bool
}

func NewStore(bucketList []string, path string, dbFolder string, readOnly bool) (*Store, error) {
	s := &Store{}
	s.bucketList = bucketList
	s.readOnly = readOnly

	// Create dir if not exist
	_ = storage.CreateDir(path)

	// Open DB
	db, err := leveldb.OpenFile(
		fmt.Sprintf("%s/%s", strings.TrimSuffix(path, "/"), dbFolder),
		&opt.Options{
			ReadOnly:               readOnly,
			OpenFilesCacheCapacity: 256,
		},
	)

	if err != nil {
		return s, err
	}

	s.db = db
	return s, nil
}

func (s *Store) CloseStore() error {
	return s.db.Close()
}

func (s *Store) SyncStore() {
	// Not necessary
}

func (s *Store) Set(bucketName []byte, k []byte, v []byte) ([]byte, error) {
	if s.readOnly {
		return nil, errors.New("readonly mod active")
	}

	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	if len(k) == 0 || len(v) == 0 {
		return nil, errors.New("key or value not found")
	}

	gkey := storage.GenerateKey(bucketName, k)

	err := s.db.Put([]byte(gkey), v, nil)

	return k, err
}

// TODO
func (s *Store) MSet(bucketName []byte, k []byte, v []byte) ([]byte, error) {
	return []byte(""), errors.New("not implemented")
}

func (s *Store) Get(bucketName []byte, k []byte) ([]byte, error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	gkey := storage.GenerateKey(bucketName, k)

	v, err := s.db.Get([]byte(gkey), nil)
	if err == leveldb.ErrNotFound {
		return v, nil
	}

	return v, err
}

func (s *Store) MGet(bucketName []byte, keys ...[]byte) (list map[string]interface{}, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	items := make(map[string]interface{})

	for _, k := range keys {
		gkey := storage.GenerateKey(bucketName, k)

		v, err := s.db.Get([]byte(gkey), nil)
		if err == leveldb.ErrNotFound {
			continue
		} else if err != nil {
			continue
		}

		items[string(k)] = string(v)
	}

	return items, nil
}

// order by asc
func (s *Store) List(bucketName []byte, k []byte, perpage int) (list []string, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	counter := 1

	items := []string{}

	prefix := storage.GenerateKey(bucketName, []byte(""))

	c := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)

	if len(k) > 0 {
		gkey := storage.GenerateKey(bucketName, k)
		for ok := c.Seek([]byte(gkey)); ok; ok = c.Next() {

			if bytes.Equal([]byte(gkey), c.Key()) {
				continue
			}

			items = append(items, string(c.Value()))

			if counter >= perpage {
				break
			}

			counter++
		}
	} else {
		for c.Next() {

			items = append(items, string(c.Value()))

			if counter >= perpage {
				break
			}

			counter++
		}
	}

	c.Release()
	err = c.Error()

	if len(items) == 0 {
		return nil, err
	}

	return items, err
}

// order by desc
func (s *Store) PrevList(bucketName []byte, k []byte, perpage int) (list []string, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	counter := 1

	items := []string{}

	prefix := storage.GenerateKey(bucketName, nil)

	c := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)

	if len(k) > 0 {
		gkey := storage.GenerateKey(bucketName, k)
		for ok := c.Seek([]byte(gkey)); ok; ok = c.Prev() {

			if bytes.Equal([]byte(gkey), k) {
				continue
			}

			items = append(items, string(c.Value()))

			if counter >= perpage {
				break
			}

			counter++
		}
	} else {
		for ok := c.Last(); ok; ok = c.Prev() {

			items = append(items, string(c.Value()))

			if counter >= perpage {
				break
			}

			counter++
		}
	}

	c.Release()
	err = c.Error()

	if len(items) == 0 {
		return nil, err
	}

	return items, err
}

func (s *Store) KeyExist(bucketName []byte, k []byte) (bool, error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return false, errors.New("unknown bucket name")
	}

	gkey := storage.GenerateKey(bucketName, k)

	v, err := s.db.Get([]byte(gkey), nil)
	if err == leveldb.ErrNotFound {
		return false, nil
	}

	return (len(v) > 0), err
}

func (s *Store) Delete(bucketName []byte, k []byte) error {
	if s.readOnly {
		return errors.New("readonly mod active")
	}

	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return errors.New("unknown bucket name")
	}

	if len(k) == 0 {
		return errors.New("key not found")
	}

	gkey := storage.GenerateKey(bucketName, k)

	return s.db.Delete([]byte(gkey), nil)
}

func (s *Store) HasBucket(bucketName []byte) bool {
	return storage.Contains(s.bucketList, bucketName)
}

func (s *Store) ListBucket() (buckets []string, err error) {
	return s.bucketList, nil
}

func (s *Store) DeleteBucket(bucketName []byte) error {
	if s.readOnly {
		return errors.New("readonly mod active")
	}

	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return errors.New("unknown bucket name")
	}

	prefix := storage.GenerateKey(bucketName, nil)

	c := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for c.Next() {
		// Use key/value.
		_ = s.db.Delete(c.Key(), nil)
	}
	c.Release()

	return c.Error()
}

func (s *Store) Backup(path, filename string) error {
	return errors.New("not implemented")
}

func (s *Store) Restore(path, filename string) error {
	return errors.New("not implemented")
}
