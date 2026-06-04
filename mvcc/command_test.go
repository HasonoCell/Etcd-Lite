package mvcc

import (
	"errors"
	"testing"
)

func TestCommandEncodeDecodeRoundTrip(t *testing.T) {
	command := Command{
		ID:   RequestID{ClientID: 7, RequestID: 42},
		Kind: CommandPut,
		Put: &PutCommand{
			Key:     []byte("/app/config"),
			Value:   []byte("v1"),
			LeaseID: 99,
			PrevKV:  true,
		},
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if decoded.ID != command.ID {
		t.Fatalf("id = %+v, want %+v", decoded.ID, command.ID)
	}
	if decoded.Kind != CommandPut {
		t.Fatalf("kind = %q, want put", decoded.Kind)
	}
	if string(decoded.Put.Key) != "/app/config" || string(decoded.Put.Value) != "v1" {
		t.Fatalf("decoded put = %+v", decoded.Put)
	}
	if decoded.Put.LeaseID != 99 || !decoded.Put.PrevKV {
		t.Fatalf("decoded put metadata = %+v", decoded.Put)
	}
}

func TestCompactCommandEncodeDecodeRoundTrip(t *testing.T) {
	command := Command{
		ID:      RequestID{ClientID: 8, RequestID: 1},
		Kind:    CommandCompact,
		Compact: &CompactCommand{Revision: 11},
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("EncodeCommand compact: %v", err)
	}
	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand compact: %v", err)
	}
	if decoded.Kind != CommandCompact || decoded.Compact == nil || decoded.Compact.Revision != 11 {
		t.Fatalf("decoded compact command = %+v", decoded)
	}
}

func TestCommandValidateRejectsInvalidPayload(t *testing.T) {
	_, err := EncodeCommand(Command{Kind: CommandPut})
	if !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("missing put payload error = %v, want ErrInvalidCommand", err)
	}

	_, err = EncodeCommand(Command{
		Kind:        CommandDeleteRange,
		DeleteRange: &DeleteRangeCommand{Key: []byte("z"), End: []byte("a")},
	})
	if !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("invalid range error = %v, want ErrInvalidRange", err)
	}
}

func TestPrefixEnd(t *testing.T) {
	if got := string(PrefixEnd([]byte("/app/"))); got != "/app0" {
		t.Fatalf("PrefixEnd = %q, want /app0", got)
	}
}
