package awstunnel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/awstunnel/pb"
)

func TestEncodeDecodeFrame(t *testing.T) {
	t.Run("roundtrip_data", func(t *testing.T) {
		orig := &pb.Message{
			Type:     pb.Message_DATA,
			StreamId: 1,
			Payload:  []byte("test data"),
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)
		require.True(t, len(frame) > 2)

		decoded, consumed, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.Equal(t, len(frame), consumed)
		assert.Equal(t, pb.Message_DATA, decoded.GetType())
		assert.Equal(t, int32(1), decoded.GetStreamId())
		assert.Equal(t, []byte("test data"), decoded.GetPayload())
	})

	t.Run("roundtrip_stream_start", func(t *testing.T) {
		orig := &pb.Message{
			Type:      pb.Message_STREAM_START,
			StreamId:  42,
			ServiceId: "SSH",
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)

		decoded, _, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.Equal(t, pb.Message_STREAM_START, decoded.GetType())
		assert.Equal(t, int32(42), decoded.GetStreamId())
		assert.Equal(t, "SSH", decoded.GetServiceId())
	})

	t.Run("roundtrip_service_ids", func(t *testing.T) {
		orig := &pb.Message{
			Type:                pb.Message_SERVICE_IDS,
			AvailableServiceIds: []string{"SSH", "HTTP"},
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)

		decoded, _, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.Equal(t, pb.Message_SERVICE_IDS, decoded.GetType())
		assert.Equal(t, []string{"SSH", "HTTP"}, decoded.GetAvailableServiceIds())
	})

	t.Run("roundtrip_connection_start", func(t *testing.T) {
		orig := &pb.Message{
			Type:         pb.Message_CONNECTION_START,
			StreamId:     1,
			ServiceId:    "SSH",
			ConnectionId: 100,
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)

		decoded, _, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.Equal(t, pb.Message_CONNECTION_START, decoded.GetType())
		assert.Equal(t, int32(1), decoded.GetStreamId())
		assert.Equal(t, "SSH", decoded.GetServiceId())
		assert.Equal(t, uint32(100), decoded.GetConnectionId())
	})

	t.Run("roundtrip_stream_reset", func(t *testing.T) {
		orig := &pb.Message{
			Type:     pb.Message_STREAM_RESET,
			StreamId: 5,
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)

		decoded, _, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.Equal(t, pb.Message_STREAM_RESET, decoded.GetType())
		assert.Equal(t, int32(5), decoded.GetStreamId())
	})

	t.Run("multiple_frames", func(t *testing.T) {
		msg1 := &pb.Message{Type: pb.Message_DATA, StreamId: 1, Payload: []byte("a")}
		msg2 := &pb.Message{Type: pb.Message_DATA, StreamId: 2, Payload: []byte("b")}

		frame1, err := EncodeFrame(msg1)
		require.NoError(t, err)

		frame2, err := EncodeFrame(msg2)
		require.NoError(t, err)

		combined := append(frame1, frame2...)

		decoded1, consumed1, err := DecodeFrame(combined)
		require.NoError(t, err)
		assert.Equal(t, int32(1), decoded1.GetStreamId())

		decoded2, consumed2, err := DecodeFrame(combined[consumed1:])
		require.NoError(t, err)
		assert.Equal(t, int32(2), decoded2.GetStreamId())
		assert.Equal(t, len(combined), consumed1+consumed2)
	})

	t.Run("frame_too_short", func(t *testing.T) {
		_, _, err := DecodeFrame([]byte{0x00})
		assert.Error(t, err)
	})

	t.Run("incomplete_frame", func(t *testing.T) {
		_, _, err := DecodeFrame([]byte{0x00, 0x0A, 0x01})
		assert.Error(t, err)
	})

	t.Run("ignorable_flag", func(t *testing.T) {
		orig := &pb.Message{
			Type:      pb.Message_DATA,
			StreamId:  1,
			Ignorable: true,
		}

		frame, err := EncodeFrame(orig)
		require.NoError(t, err)

		decoded, _, err := DecodeFrame(frame)
		require.NoError(t, err)
		assert.True(t, decoded.GetIgnorable())
	})
}
