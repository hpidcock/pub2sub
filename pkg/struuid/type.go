package struuid

import (
	"github.com/google/uuid"
)

// UUID represents a textual uuid
type UUID string

const (
	Nil UUID = "00000000-0000-0000-0000-000000000000"
)

func FromUUID(u uuid.UUID) UUID {
	return UUID(u.String())
}

func Parse(s string) (UUID, error) {
	parsedUUID, err := uuid.Parse(s)
	if err != nil {
		return Nil, err
	}

	return UUID(parsedUUID.String()), nil
}

func Must(u UUID, err error) UUID {
	if err != nil {
		panic(err)
	}
	return u
}

func (u UUID) String() string {
	return string(u)
}
