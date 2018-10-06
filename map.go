package tiny

import (
	"github.com/boltdb/bolt"
	"reflect"
	"sync"
	"errors"
	"encoding/json"
)

type Map struct {
	store   Store
	mutex   sync.Mutex
	vtype   reflect.Type
	data    map[string]interface{}
}

func (s Store) OpenMap(name string, mode Mode, v interface{}) (m *Map, err error) {

	if reflect.TypeOf(v).Kind() == reflect.Ptr {
		return nil, errors.New("pointer types are not allowed")
	}

	err = s.db.Update(func(tx *bolt.Tx) error {

		b, err := s.gotoBucket(tx)
		if err != nil {
			return err
		}

		err = ensureStoreSchema(b, name, "map")
		if err != nil {
			return err
		}

		m = &Map{
			store: Store{
				db:   s.db,
				path: append(s.path, name),
				mode: mode,
			},
			data:  make(map[string]interface{}),
			vtype: reflect.TypeOf(v),
		}

		// if memory mode is enabled preload everything
		if m.store.mode == ModeMem {
			valueBucket, err := m.store.gotoValueBucket(tx)
			if err != nil {
				return err
			}
			if err := valueBucket.ForEach(func(k, data []byte) error {
				v := reflect.New(m.vtype).Interface()
				err := json.Unmarshal(data, v)
				if err != nil {
					return err
				}
				m.data[string(k)] = v
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Map) Store() Store {
	return m.store
}

func (m *Map) Foreach(f func(k string, v interface{}) error) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// in memory mode the mapped data is complete and can be iterated directly
	if m.store.mode == ModeMem {
		for k, v := range m.data {
			err := f(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return m.store.db.View(func(tx *bolt.Tx) error {

		b, err := m.store.gotoValueBucket(tx)
		if err != nil {
			return err
		}

		if m.store.mode == ModeLazy {
			return b.ForEach(func(k, data []byte) error {
				key := string(k)

				// try to get value from map data
				if v, exist := m.data[key]; exist {
					err := f(key, v)
					if err != nil {
						return err
					}
					return nil
				}

				// fallback to disk and write value to map data
				v := reflect.New(m.vtype).Interface()
				err := json.Unmarshal(data, v)
				if err != nil {
					return err
				}
				m.data[string(k)] = v

				err = f(key, v)
				if err != nil {
					return err
				}
				return nil
			})
		}
		return b.ForEach(func(k, data []byte) error {
			v := reflect.New(m.vtype).Interface()
			err := json.Unmarshal(data, v)
			if err != nil {
				return err
			}
			return f(string(k), v)
		})
	})
}

func (m *Map) Get(k string) interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// try to get value from map data
	if m.store.mode == ModeMem || m.store.mode == ModeLazy {
		if value, exist := m.data[k]; exist {
			return value
		} else if m.store.mode == ModeMem {

			// give up if using memory mode
			return nil
		}
	}

	// load value from disk
	var v interface{}
	if err := m.store.db.View(func(tx *bolt.Tx) error {

		b, err := m.store.gotoValueBucket(tx)
		if err != nil {
			return err
		}

		data := b.Get([]byte(k))
		if data == nil {
			return errors.New("key '" + k + "' does not exist")
		}

		v = reflect.New(m.vtype).Interface()

		err = json.Unmarshal(data, v)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil
	}

	// save in map data if using lazy mode
	if m.store.mode == ModeLazy {
		m.data[string(k)] = v
	}

	return v
}

func (m *Map) Set(k string, v interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if v == nil {

		// remove from database
		if err := m.store.db.Update(func(tx *bolt.Tx) error {

			b, err := m.store.gotoValueBucket(tx)
			if err != nil {
				return err
			}

			return b.Delete([]byte(k))
		}); err != nil {
			return err
		}

		// remove from memory
		delete(m.data, k)

		return nil
	}

	// validate pointer of vtype
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr || t.Elem() != m.vtype {
		return errors.New("value must be of type '*" + m.vtype.String() + "'")
	}

	// serialize and save
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err = m.store.db.Update(func(tx *bolt.Tx) error {
		b, err := m.store.gotoValueBucket(tx)
		if err != nil {
			return err
		}

		return b.Put([]byte(k), data)
	}); err != nil {
		return err
	}

	// write to map data if using memory or lazy mode
	if m.store.mode == ModeMem || m.store.mode == ModeLazy {
		m.data[string(k)] = v
	}

	return nil
}
