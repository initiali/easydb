package easydb

const (
	// offset64 FNVa offset basis.
	// See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	offset64 = 14695981039346656037
	// prime64 FNVa prime value.
	// See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	prime64 = 1099511628211
)

type Hashed interface {
	Sum64([]byte) uint64
}

func DefaultHashFunc() Hashed {
	return fnv64a{}
}
