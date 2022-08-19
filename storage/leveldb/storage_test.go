package leveldbstorage

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCmd(t *testing.T) {
	store, err := OpenStore()
	assert.NoError(t, err)

	key, err := store.Set([]byte("posts"), []byte("test_1"), []byte("number one"))
	assert.Equal(t, true, bytes.Equal(key, []byte("test_1")))
	assert.NoError(t, err)

	key, err = store.Set([]byte("posts"), []byte("test_2"), []byte("number two"))
	assert.Equal(t, true, bytes.Equal(key, []byte("test_2")))
	assert.NoError(t, err)

	_, err = store.MSet([]byte("posts"), []byte("test_1"), []byte("number one"))
	assert.Error(t, err)

	res, err := store.Get([]byte("posts"), []byte("test_1"))
	assert.Equal(t, true, bytes.Equal(res, []byte("number one")))
	assert.NoError(t, err)

	var keys [][]byte
	keys = append(keys, []byte("test_1"))
	keys = append(keys, []byte("test_2"))

	items, err := store.MGet([]byte("posts"), keys...)
	assert.NoError(t, err)
	assert.Equal(t, items, map[string]interface{}{"test_1": "number one", "test_2": "number two"})

	list, err := store.List([]byte("posts"), nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, list, []string{"number one", "number two"})

	list, err = store.List([]byte("posts"), []byte("test_1"), 10)
	assert.NoError(t, err)
	assert.Equal(t, list, []string{"number two"})

	list, err = store.PrevList([]byte("posts"), nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, list, []string{"number two", "number one"})

	list, err = store.PrevList([]byte("posts"), []byte("test_1"), 10)
	assert.NoError(t, err)
	assert.Equal(t, list, []string{"number one"})

	ok, err := store.KeyExist([]byte("posts"), []byte("test_1"))
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = store.KeyExist([]byte("posts"), []byte("test_3"))
	assert.False(t, ok)
	assert.NoError(t, err)

	err = store.Delete([]byte("posts"), []byte("test_1"))
	assert.NoError(t, err)

	res, err = store.Get([]byte("posts"), []byte("test_1"))
	assert.Equal(t, true, bytes.Equal(res, nil))
	assert.NoError(t, err)

	err = store.DeleteBucket([]byte("posts"))
	assert.NoError(t, err)

	list, err = store.List([]byte("posts"), nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, list, []string(nil))

	err = store.CloseStore()
	assert.NoError(t, err)

	err = DeleteStore()
	assert.NoError(t, err)
}

func OpenStore() (*Store, error) {
	return NewStore([]string{"options", "posts", "pages"}, "./db/", "storage_test", false)
}

func DeleteStore() error {
	return os.RemoveAll("./db/storage_test")
}
