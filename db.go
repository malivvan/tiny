package tiny

import (
	"github.com/boltdb/bolt"
	"time"
)

type DB struct {
	db    *bolt.DB
}

func (db *DB) Store() Store {
	return Store{
		path: []string{},
		db:   db.db,
	}
}

func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		return ensureDBSchema(tx)
	})
	if err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

