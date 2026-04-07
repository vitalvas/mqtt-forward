package awstunnel

import (
	"encoding/binary"
	"errors"

	"github.com/vitalvas/mqtt-forward/internal/awstunnel/pb"
	"google.golang.org/protobuf/proto"
)

func EncodeFrame(msg *pb.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(frame[:2], uint16(len(data)))
	copy(frame[2:], data)

	return frame, nil
}

func DecodeFrame(data []byte) (*pb.Message, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("frame too short")
	}

	length := int(binary.BigEndian.Uint16(data[:2]))

	if len(data) < 2+length {
		return nil, 0, errors.New("incomplete frame")
	}

	msg := &pb.Message{}
	if err := proto.Unmarshal(data[2:2+length], msg); err != nil {
		return nil, 0, err
	}

	return msg, 2 + length, nil
}
