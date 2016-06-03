package proto

import (
	"errors"

	"github.com/gholt/store"
)

func TranslateError(err error) string {
	if store.IsDisabled(err) {
		return "::github.com/gholt/store/ErrDisabled::"
	} else if store.IsNotFound(err) {
		return "::github.com/gholt/store/ErrNotFound::"
	}
	return err.Error()
}

func TranslateErrorString(errstr string) error {
	switch errstr {
	case "::github.com/gholt/store/ErrDisabled::":
		return errDisabled
	case "::github.com/gholt/store/ErrNotFound::":
		return errNotFound
	}
	return errors.New(errstr)
}

var errDisabled error = _errDisabled{}

type _errDisabled struct{}

func (e _errDisabled) Error() string { return "remote store disabled" }

func (e _errDisabled) ErrDisabled() string { return "remote store disabled" }

var errNotFound error = _errNotFound{}

type _errNotFound struct{}

func (e _errNotFound) Error() string { return "not found by remote store" }

func (e _errNotFound) ErrNotFound() string { return "not found by remote store" }
