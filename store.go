package tiny

import (
	"github.com/boltdb/bolt"
	"errors"
	"strings"
	"bytes"
)

type Store struct {
	db   *bolt.DB
	path []string
	mode Mode
}

func (s Store) Remove(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := s.gotoBucket(tx)
		if err != nil {
			return err
		}
		err = b.DeleteBucket([]byte(name))
		if err != nil {
			return errors.New("store '" + name + "' not found")
		}
		return nil
	})
}

func (s Store) List() ([]string, error) {
	list := []string{}
	s.db.View(func(tx *bolt.Tx) error {
		b, err := s.gotoBucket(tx)
		if err != nil {
			return err
		}
		return b.ForEach(func(k, v []byte) error {
			if bytes.Equal(k, storeMetaBucketKey) || bytes.Equal(k, storeValueBucketKey) {
				return nil
			}
			list = append(list, string(k))
			return nil
		})
	})
	return list, nil
}

func (s Store) gotoValueBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	b, err := s.gotoBucket(tx)
	if err != nil {
		return nil, err
	}
	return b.Bucket(storeValueBucketKey), nil
}

func (s Store) gotoBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	b := tx.Bucket(dbRootBucketKey)
	for i, seg := range s.path {
		b = b.Bucket([]byte(seg))
		if b == nil {
			return nil, errors.New("bucket '" + strings.Join(s.path[:i], "/") + "' does not exist")
		}
	}
	return b, nil
}
