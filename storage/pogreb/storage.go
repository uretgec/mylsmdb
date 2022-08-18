package pogrebstorage

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/uretgec/mylsmdb/storage"

	"github.com/akrylysov/pogreb"
)

type Store struct {
	db         *pogreb.DB
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
	db, err := pogreb.Open(
		fmt.Sprintf("%s%s", path, dbFolder),
		&pogreb.Options{
			BackgroundSyncInterval: -1, // every write operation sync trigger
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

// BackgroundSyncInterval option enabled. Not neccessary to call
func (s *Store) SyncStore() {
	s.db.Sync()
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

	err := s.db.Put([]byte(gkey), v)

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

	v, err := s.db.Get([]byte(gkey))

	return v, err
}

func (s *Store) MGet(bucketName []byte, keys ...[]byte) (list map[string]interface{}, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	items := make(map[string]interface{})

	for _, k := range keys {
		gkey := storage.GenerateKey(bucketName, k)

		v, err := s.db.Get([]byte(gkey))
		if err == leveldb.ErrNotFound {
			continue
		} else if err != nil {
			continue
		}

		items[gkey] = string(v)
	}

	return items, nil
}

/*
First()  Move to the first key.
Last()   Move to the last key.
Seek()   Move to a specific key.
Next()   Move to the next key.
Prev()   Move to the previous key.
*/
func (s *Store) List(bucketName []byte, k []byte, perpage int) (list []string, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	counter := 1

	items := []string{}

	prefix := storage.GenerateKey(bucketName, nil)

	c := s.db.Items()

	if len(k) > 0 {
		gkey := storage.GenerateKey(bucketName, k)

		for {
			key, val, err := c.Next()
			if err == pogreb.ErrIterationDone {
				break
			}

			if !bytes.HasPrefix([]byte(prefix), key) {
				continue
			}

			if bytes.Equal([]byte(gkey), k) {
				continue
			}

			items = append(items, string(val))

			if counter >= perpage {
				break
			}

			counter++
		}
	} else {
		for {
			key, val, err := c.Next()
			if err == pogreb.ErrIterationDone {
				break
			}

			if !bytes.HasPrefix([]byte(prefix), key) {
				continue
			}

			items = append(items, string(val))

			if counter >= perpage {
				break
			}

			counter++
		}
	}

	if len(items) == 0 {
		return nil, err
	}

	return items, err
}

func (s *Store) PrevList(bucketName []byte, k []byte, perpage int) (list []string, err error) {
	return nil, errors.New("not implemented")
}

func (s *Store) KeyExist(bucketName []byte, k []byte) (bool, error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return false, errors.New("unknown bucket name")
	}

	gkey := storage.GenerateKey(bucketName, k)

	return s.db.Has([]byte(gkey))
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

	return s.db.Delete([]byte(gkey))
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

	var err error
	var key []byte

	c := s.db.Items()
	for {
		key, _, err = c.Next()
		if err == pogreb.ErrIterationDone {
			break
		}

		if err != nil {
			break
		}

		if !bytes.HasPrefix([]byte(prefix), key) {
			continue
		}

		err = s.db.Delete(key)
		if err != nil {
			break
		}

	}

	return err
}

func (s *Store) Backup(path, filename string) error {
	return errors.New("not implemented")
}

func (s *Store) Restore(path, filename string) error {
	return errors.New("not implemented")
}
