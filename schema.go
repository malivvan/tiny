package tiny

import (
	"errors"
	"github.com/boltdb/bolt"
	"time"
	"bytes"
)

func ensureDBSchema(tx *bolt.Tx) (error) {
	if tx.Bucket(dbMetaBucketKey) != nil && tx.Bucket(dbRootBucketKey) != nil {
		return nil
	}
	if tx.Bucket(dbRootBucketKey) != nil {
		return errors.New("database corrupted (root bucket missing)")
	}
	if tx.Bucket(dbMetaBucketKey) != nil {
		return errors.New("database corrupted (meta bucket missing)")
	}

	meta, err := tx.CreateBucket(dbMetaBucketKey)
	if err != nil {
		return err
	}
	created, err := time.Now().MarshalBinary()
	if err != nil {
		return err
	}
	err = meta.Put(dbMetaCreatedKey, created)
	if err != nil {
		return err
	}

	_, err = tx.CreateBucket(dbRootBucketKey)
	if err != nil {
		return err
	}

	return nil
}

func ensureStoreSchema(b *bolt.Bucket, name string, t string) error {
	if existingBucket := b.Bucket([]byte(name)); existingBucket != nil {

		// validate base schema
		meta := existingBucket.Bucket(storeMetaBucketKey)
		if meta == nil {
			return errors.New("meta bucket does not exist")
		}
		if existingBucket.Bucket(storeValueBucketKey) == nil {
			return errors.New("value bucket does not exist")
		}

		// validate schema type
		metaType := meta.Get(storeMetaBucketTypeKey)
		if metaType == nil {
			return errors.New("meta bucket type key not set")
		}
		if !bytes.Equal(metaType, []byte(t)) {
			return errors.New("cannot open " + string(metaType) + " as " + t)
		}
	} else {

		// create base schema
		b, err := b.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		meta, err := b.CreateBucket(storeMetaBucketKey)
		if err != nil {
			return err
		}
		_, err = b.CreateBucket(storeValueBucketKey)
		if err != nil {
			return err
		}

		// write type
		err = meta.Put(storeMetaBucketTypeKey, []byte(t))
		if err != nil {
			return err
		}
	}
	return nil
}
