package nltest_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/mdlayher/netlink"
	"github.com/mdlayher/netlink/nltest"
)

func TestConnSend(t *testing.T) {
	req := netlink.Message{
		Data: []byte{0xff, 0xff, 0xff, 0xff},
	}

	c := nltest.Dial(func(creq netlink.Message) ([]netlink.Message, error) {
		if want, got := req.Data, creq.Data; !bytes.Equal(want, got) {
			t.Fatalf("unexpected request data:\n- want: %v\n-  got: %v",
				want, got)
		}

		return nil, nil
	})
	defer c.Close()

	if _, err := c.Send(req); err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
}

func TestConnReceiveMulticast(t *testing.T) {
	msgs := []netlink.Message{{
		Data: []byte{0xff, 0xff, 0xff, 0xff},
	}}

	c := nltest.Dial(func(zero netlink.Message) ([]netlink.Message, error) {
		if want, got := (netlink.Message{}), zero; !reflect.DeepEqual(want, got) {
			t.Fatalf("unexpected zero message:\n- want: %v\n-  got: %v",
				want, got)
		}

		return msgs, nil
	})
	defer c.Close()

	got, err := c.Receive()
	if err != nil {
		t.Fatalf("failed to receive messages: %v", err)
	}

	if want := msgs; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected multicast messages:\n- want: %v\n-  got: %v",
			want, got)
	}
}

func TestConnExecuteOK(t *testing.T) {
	req := netlink.Message{
		Header: netlink.Header{
			Length:   16,
			Flags:    netlink.HeaderFlagsRequest,
			Sequence: 1,
			PID:      1,
		},
	}

	c := nltest.Dial(func(creq netlink.Message) ([]netlink.Message, error) {
		// Turn the request back around to the client.
		return []netlink.Message{creq}, nil
	})
	defer c.Close()

	got, err := c.Execute(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}

	if want := []netlink.Message{req}; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected response messages:\n- want: %v\n-  got: %v",
			want, got)
	}
}

func TestConnExecuteMultipartOK(t *testing.T) {
	req := netlink.Message{
		Header: netlink.Header{
			Length:   16,
			Flags:    netlink.HeaderFlagsRequest,
			Sequence: 1,
			PID:      1,
		},
	}

	c := nltest.Dial(func(creq netlink.Message) ([]netlink.Message, error) {
		// Client should only receive one message with multipart flag set.
		return nltest.Multipart([]netlink.Message{
			creq, creq,
		})
	})
	defer c.Close()

	got, err := c.Execute(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}

	req.Header.Flags |= netlink.HeaderFlagsMulti
	if want := []netlink.Message{req}; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected response messages:\n- want: %v\n-  got: %v",
			want, got)
	}
}

func TestConnExecuteError(t *testing.T) {
	err := errors.New("foo")

	c := nltest.Dial(func(creq netlink.Message) ([]netlink.Message, error) {
		// Error should be surfaced by Execute's call to Receive.
		return nil, err
	})
	defer c.Close()

	_, got := c.Execute(netlink.Message{})
	if want := err; want != got {
		t.Fatalf("unexpected error:\n- want: %v\n-  got: %v",
			want, got)
	}
}

func TestError(t *testing.T) {
	const (
		eperm  = 1
		enoent = 2
	)

	tests := []struct {
		name   string
		number int
		in     netlink.Message
		out    []netlink.Message
	}{
		{
			name:   "EPERM",
			number: eperm,
			in: netlink.Message{
				Header: netlink.Header{
					Length:   24,
					Flags:    netlink.HeaderFlagsRequest | netlink.HeaderFlagsDump,
					Sequence: 10,
					PID:      1000,
				},
				Data: []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
			},
			out: []netlink.Message{{
				Header: netlink.Header{
					Length:   28,
					Type:     netlink.HeaderTypeError,
					Flags:    netlink.HeaderFlagsRequest | netlink.HeaderFlagsDump,
					Sequence: 10,
					PID:      1000,
				},
				Data: []byte{
					0xff, 0xff, 0xff, 0xff,
					0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
				},
			}},
		},
		{
			name:   "ENOENT",
			number: enoent,
			in: netlink.Message{
				Header: netlink.Header{
					Length:   20,
					Flags:    netlink.HeaderFlagsRequest,
					Sequence: 1,
					PID:      100,
				},
				Data: []byte{0x11, 0x22, 0x33, 0x44},
			},
			out: []netlink.Message{{
				Header: netlink.Header{
					Length:   24,
					Type:     netlink.HeaderTypeError,
					Flags:    netlink.HeaderFlagsRequest,
					Sequence: 1,
					PID:      100,
				},
				Data: []byte{
					0xfe, 0xff, 0xff, 0xff,
					0x11, 0x22, 0x33, 0x44,
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := nltest.Error(tt.number, tt.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if want, got := tt.out, out; !reflect.DeepEqual(want, got) {
				t.Fatalf("unexpected output messages:\n- want: %v\n-  got: %v",
					want, got)
			}
		})
	}
}

func TestMultipart(t *testing.T) {
	tests := []struct {
		name string
		in   []netlink.Message
		out  []netlink.Message
	}{
		{
			name: "no messages",
		},
		{
			name: "one message, no changes",
			in: []netlink.Message{{
				Header: netlink.Header{
					Length: 20,
				},
				Data: []byte{0xff, 0xff, 0xff, 0xff},
			}},
			out: []netlink.Message{{
				Header: netlink.Header{
					Length: 20,
				},
				Data: []byte{0xff, 0xff, 0xff, 0xff},
			}},
		},
		{
			name: "two messages, multipart",
			in: []netlink.Message{
				{
					Header: netlink.Header{
						Length: 20,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 16,
					},
				},
			},
			out: []netlink.Message{
				{
					Header: netlink.Header{
						Length: 20,
						Flags:  netlink.HeaderFlagsMulti,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 16,
						Type:   netlink.HeaderTypeDone,
						Flags:  netlink.HeaderFlagsMulti,
					},
				},
			},
		},
		{
			name: "three messages, multipart",
			in: []netlink.Message{
				{
					Header: netlink.Header{
						Length: 20,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 24,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 16,
					},
				},
			},
			out: []netlink.Message{
				{
					Header: netlink.Header{
						Length: 20,
						Flags:  netlink.HeaderFlagsMulti,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 24,
						Flags:  netlink.HeaderFlagsMulti,
					},
					Data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
				{
					Header: netlink.Header{
						Length: 16,
						Type:   netlink.HeaderTypeDone,
						Flags:  netlink.HeaderFlagsMulti,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := nltest.Multipart(tt.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if want, got := tt.out, out; !reflect.DeepEqual(want, got) {
				t.Fatalf("unexpected output messages:\n- want: %v\n-  got: %v",
					want, got)
			}
		})
	}
}
