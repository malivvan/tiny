package tiny

import (
	"github.com/boltdb/bolt"
	"reflect"
	"sync"
	"errors"
	"encoding/json"
)

type Map struct {
	store Store
	mutex sync.Mutex
	vtype reflect.Type
	data  map[string]interface{}
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

func (m *Map) Contains(k string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.store.mode == ModeMem {
		if _, exist := m.data[k]; exist {
			return true
		}
		return false
	}

	exist := false
	if err := m.store.db.View(func(tx *bolt.Tx) error {

		b, err := m.store.gotoValueBucket(tx)
		if err != nil {
			return err
		}

		if b.Get([]byte(k)) != nil {
			exist = true
		}

		return nil
	}); err != nil {
		return false
	}
	return exist
}

func (m *Map) Get(k string) (interface{}, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// memory mode: get value from map data
	if m.store.mode == ModeMem {
		if value, exist := m.data[k]; exist {
			return value, nil
		}
		return nil, errors.New("key '"+k+"' does not exist")
	}

	// disk mode: load value from disk
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
		return nil, err
	}

	return v, nil
}

func (m *Map) Put(k string, v interface{}) error {
	if v == nil {
		return errors.New("value cannot be nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	
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

	// write to map data if using memory mode
	if m.store.mode == ModeMem {
		m.data[string(k)] = v
	}

	return nil
}

func (m *Map) Remove(k string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

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
	if m.store.mode == ModeMem {
		delete(m.data, k)
	}

	return nil
}