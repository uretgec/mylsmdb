package nutsdbstorage

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/uretgec/mylsmdb/storage"
	"github.com/xujiajun/nutsdb"
)

type Store struct {
	db         *nutsdb.DB
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
	db, err := nutsdb.Open(
		nutsdb.Options{
			EntryIdxMode:         nutsdb.HintKeyAndRAMIdxMode,
			SegmentSize:          nutsdb.DefaultOptions.SegmentSize,
			NodeNum:              1,
			RWMode:               nutsdb.FileIO,
			SyncEnable:           true,
			StartFileLoadingMode: nutsdb.FileIO,
		},
		nutsdb.WithDir(fmt.Sprintf("%s/%s", strings.TrimSuffix(path, "/"), dbFolder)),
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

	err := s.db.Update(func(t *nutsdb.Tx) error {
		return t.Put(string(bucketName), k, v, 0)
	})

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

	var item []byte
	err := s.db.View(func(t *nutsdb.Tx) error {
		rxData, err := t.Get(string(bucketName), k)
		if err == nutsdb.ErrNotFoundKey {
			return nil
		} else if err != nil {
			return err
		}

		if len(rxData.Value) > 0 {
			item = rxData.Value
		}

		return nil
	})

	return item, err
}

func (s *Store) MGet(bucketName []byte, keys ...[]byte) (list map[string]interface{}, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	items := make(map[string]interface{})

	err = s.db.View(func(t *nutsdb.Tx) error {
		for _, key := range keys {
			rxData, err := t.Get(string(bucketName), key)
			if err == nutsdb.ErrNotFoundKey {
				continue
			} else if err != nil {
				continue
			}

			items[string(key)] = string(rxData.Value)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return items, nil
}

// order by asc
func (s *Store) List(bucketName []byte, k []byte, perpage int) (list []string, err error) {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return nil, errors.New("unknown bucket name")
	}

	//return []string{fmt.Sprintf("%d", s.statsBucket(bucketName))}, nil
	// line 162 - when bucket is empty and call c.SetNext, boom. its huge bug!
	if s.statsBucket(bucketName) == 0 {
		return nil, nil
	}

	counter := 1

	items := []string{}

	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}

	c := nutsdb.NewIterator(tx, string(bucketName))
	if len(k) > 0 {
		err = c.Seek(k)
		if err != nil {
			return nil, err
		}
	}

	for {
		ok, err := c.SetNext()
		if !ok {
			err = nil
			break
		}

		if err != nil {
			break
		}

		if bytes.Equal(k, c.Entry().Key) {
			continue
		}

		items = append(items, string(c.Entry().Value))

		if counter >= perpage {
			break
		}

		counter++
	}

	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
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

	var exists bool
	err := s.db.View(func(t *nutsdb.Tx) error {
		rxData, err := t.Get(string(bucketName), k)
		if err == nutsdb.ErrNotFoundKey {
			return nil
		} else if err == nutsdb.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		if len(rxData.Value) > 0 {
			exists = true
		}

		return nil
	})

	return exists, err
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

	return s.db.Update(func(t *nutsdb.Tx) error {
		return t.Delete(string(bucketName), k)
	})
}

func (s *Store) HasBucket(bucketName []byte) bool {
	return storage.Contains(s.bucketList, bucketName)
}

func (s *Store) statsBucket(bucketName []byte) int {
	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return 0
	}

	treeIdx, ok := s.db.BPTreeIdx[string(bucketName)]
	if !ok {
		return 0
	}

	return treeIdx.ValidKeyCount
}

func (s *Store) ListBucket() (buckets []string, err error) {
	bucketList := []string{}

	err = s.db.View(func(t *nutsdb.Tx) error {
		return t.IterateBuckets(nutsdb.DataStructureBPTree, func(name string) {
			bucketList = append(bucketList, string(name))
		})
	})

	if err != nil {
		return nil, err
	}

	return bucketList, nil
}

func (s *Store) DeleteBucket(bucketName []byte) error {
	if s.readOnly {
		return errors.New("readonly mod active")
	}

	if len(bucketName) > 0 && !storage.Contains(s.bucketList, bucketName) {
		return errors.New("unknown bucket name")
	}

	return s.db.Update(func(t *nutsdb.Tx) error {
		err := t.DeleteBucket(nutsdb.DataStructureBPTree, string(bucketName))
		if err != nil {
			return err
		}

		return t.DeleteBucket(nutsdb.DataStructureSet, string(bucketName))
	})
}

func (s *Store) Backup(path, filename string) error {
	// Create dir if necessary
	_ = storage.CreateDir(strings.TrimSuffix(path, "/"))

	return s.db.Backup(path)
}

func (s *Store) Restore(path, filename string) error {
	return errors.New("not implemented")
}
