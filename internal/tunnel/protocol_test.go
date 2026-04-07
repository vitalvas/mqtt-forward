package tunnel

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControlMessage(t *testing.T) {
	t.Run("marshal_unmarshal_open", func(t *testing.T) {
		msg := ControlMessage{
			Type:      MessageTypeOpen,
			SessionID: "sess-1",
			Mode:      SessionModeTCP,
			Target:    "localhost:8080",
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var decoded ControlMessage
		require.NoError(t, json.Unmarshal(data, &decoded))

		assert.Equal(t, msg, decoded)
	})

	t.Run("marshal_unmarshal_open_ack", func(t *testing.T) {
		msg := ControlMessage{
			Type:      MessageTypeOpenAck,
			SessionID: "sess-1",
			Success:   true,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var decoded ControlMessage
		require.NoError(t, json.Unmarshal(data, &decoded))

		assert.Equal(t, msg, decoded)
	})

	t.Run("marshal_unmarshal_close_with_exit_code", func(t *testing.T) {
		exitCode := 1
		msg := ControlMessage{
			Type:      MessageTypeClose,
			SessionID: "sess-1",
			ExitCode:  &exitCode,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var decoded ControlMessage
		require.NoError(t, json.Unmarshal(data, &decoded))

		assert.Equal(t, msg, decoded)
		require.NotNil(t, decoded.ExitCode)
		assert.Equal(t, 1, *decoded.ExitCode)
	})

	t.Run("marshal_unmarshal_resize", func(t *testing.T) {
		msg := ControlMessage{
			Type:      MessageTypeResize,
			SessionID: "sess-1",
			Cols:      120,
			Rows:      40,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var decoded ControlMessage
		require.NoError(t, json.Unmarshal(data, &decoded))

		assert.Equal(t, msg, decoded)
	})

	t.Run("omitempty_fields", func(t *testing.T) {
		msg := ControlMessage{
			Type:      MessageTypeClose,
			SessionID: "sess-1",
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		assert.NotContains(t, string(data), "mode")
		assert.NotContains(t, string(data), "target")
		assert.NotContains(t, string(data), "command")
		assert.NotContains(t, string(data), "exit_code")
	})
}

func TestTopicHelpers(t *testing.T) {
	t.Run("in_control_topic", func(t *testing.T) {
		topic := InControlTopic("device-1")
		assert.Equal(t, "tunnel/device-1/in/control", topic)
	})

	t.Run("in_data_topic", func(t *testing.T) {
		topic := InDataTopic("device-1", "sess-1")
		assert.Equal(t, "tunnel/device-1/in/data/sess-1", topic)
	})

	t.Run("out_control_topic", func(t *testing.T) {
		topic := OutControlTopic("device-1")
		assert.Equal(t, "tunnel/device-1/out/control", topic)
	})

	t.Run("out_data_topic", func(t *testing.T) {
		topic := OutDataTopic("device-1", "sess-1")
		assert.Equal(t, "tunnel/device-1/out/data/sess-1", topic)
	})

	t.Run("in_control_filter", func(t *testing.T) {
		assert.Equal(t, "tunnel/device-1/in/control", InControlFilter("device-1"))
	})

	t.Run("in_data_filter", func(t *testing.T) {
		assert.Equal(t, "tunnel/device-1/in/data/+", InDataFilter("device-1"))
	})

	t.Run("out_control_filter", func(t *testing.T) {
		assert.Equal(t, "tunnel/device-1/out/control", OutControlFilter("device-1"))
	})

	t.Run("out_data_filter", func(t *testing.T) {
		assert.Equal(t, "tunnel/device-1/out/data/+", OutDataFilter("device-1"))
	})
}

func TestParseTopic(t *testing.T) {
	t.Run("parse_in_control_topic", func(t *testing.T) {
		parsed, err := ParseTopic("tunnel/device-1/in/control")
		require.NoError(t, err)

		assert.Equal(t, "device-1", parsed.DeviceID)
		assert.True(t, parsed.IsControl)
		assert.False(t, parsed.IsData)
		assert.Empty(t, parsed.SessionID)
	})

	t.Run("parse_out_data_topic", func(t *testing.T) {
		parsed, err := ParseTopic("tunnel/device-1/out/data/sess-1")
		require.NoError(t, err)

		assert.Equal(t, "device-1", parsed.DeviceID)
		assert.False(t, parsed.IsControl)
		assert.True(t, parsed.IsData)
		assert.Equal(t, "sess-1", parsed.SessionID)
	})

	t.Run("too_short", func(t *testing.T) {
		_, err := ParseTopic("tunnel/a/b")
		assert.Error(t, err)
	})

	t.Run("invalid_prefix", func(t *testing.T) {
		_, err := ParseTopic("invalid/a/in/control")
		assert.Error(t, err)
	})

	t.Run("invalid_direction", func(t *testing.T) {
		_, err := ParseTopic("tunnel/a/bad/control")
		assert.Error(t, err)
	})

	t.Run("unknown_type", func(t *testing.T) {
		_, err := ParseTopic("tunnel/a/in/unknown")
		assert.Error(t, err)
	})

	t.Run("control_extra_segment", func(t *testing.T) {
		_, err := ParseTopic("tunnel/a/in/control/extra")
		assert.Error(t, err)
	})

	t.Run("data_missing_session", func(t *testing.T) {
		_, err := ParseTopic("tunnel/a/in/data")
		assert.Error(t, err)
	})
}

func TestDataFrame(t *testing.T) {
	t.Run("encode_decode", func(t *testing.T) {
		payload := []byte("hello world")
		frame := EncodeDataFrame(42, payload)

		seq, decoded, err := DecodeDataFrame(frame)
		require.NoError(t, err)

		assert.Equal(t, uint32(42), seq)
		assert.Equal(t, payload, decoded)
	})

	t.Run("empty_payload", func(t *testing.T) {
		frame := EncodeDataFrame(0, nil)

		seq, decoded, err := DecodeDataFrame(frame)
		require.NoError(t, err)

		assert.Equal(t, uint32(0), seq)
		assert.Empty(t, decoded)
	})

	t.Run("frame_too_short", func(t *testing.T) {
		_, _, err := DecodeDataFrame([]byte{0, 1})
		assert.Error(t, err)
	})

	t.Run("large_sequence_number", func(t *testing.T) {
		frame := EncodeDataFrame(0xFFFFFFFF, []byte{1})

		seq, _, err := DecodeDataFrame(frame)
		require.NoError(t, err)

		assert.Equal(t, uint32(0xFFFFFFFF), seq)
	})
}
