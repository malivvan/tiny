// Package tiny implements persistent data structures.
package tiny

var (
	// meta bucket and keys for the first layer
	dbMetaBucketKey  = []byte{0x00}
	dbMetaCreatedKey = []byte{0x00}

	// the root bucket on the first layer
	dbRootBucketKey = []byte{0x01}

	// meta bucket and keys for Store layers
	storeMetaBucketKey     = []byte{0x00}
	storeMetaBucketTypeKey = []byte{0x00}

	// Store bucket key for Store layers
	storeValueBucketKey = []byte{0x01}
)

type Mode int

const (
	ModeDisk = iota
	ModeMem
)
