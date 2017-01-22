package broadcast

import (
	"bytes"
	"testing"
)

func TestRTMPClient(t *testing.T) {
	url := "rtmp://a.rtmp.youtube.com:1935/live2"
	logFunc := func(format string, a ...interface{}) {
		t.Logf(format, a...)
	}

	client, err := NewRTMPClient(url, logFunc)
	if err != nil {
		t.Error(err)
	}

	err = client.Handshake()
	if err != nil {
		t.Error(err)
	}
}

func testChunk(t *testing.T, index int, chunk *rtmpChunk, expectedChunk *rtmpChunk) {
	if chunk.fmt != expectedChunk.fmt {
		t.Errorf("chunk %d: unexpected chunk fmt, got '%v', wanted '%v'", index, chunk.fmt, expectedChunk.fmt)
	}
	if chunk.streamID != expectedChunk.streamID {
		t.Errorf("chunk %d: unexpected chunk stream id, got '%v', wanted '%v'", index, chunk.streamID, expectedChunk.streamID)
	}
	if chunk.message.length != expectedChunk.message.length {
		t.Errorf("chunk %d: unexpected message length, got '%v', wanted '%v'", index, chunk.message.length, expectedChunk.message.length)
	}
	if chunk.message.messageType != expectedChunk.message.messageType {
		t.Errorf("chunk %d: unexpected message message type, got '%v', wanted '%v'", index, chunk.message.messageType, expectedChunk.message.messageType)
	}
	if chunk.message.streamID != expectedChunk.message.streamID {
		t.Errorf("chunk %d: unexpected message stream id, got '%v', wanted '%v'", index, chunk.message.streamID, expectedChunk.message.streamID)
	}
	if chunk.message.extended != expectedChunk.message.extended {
		t.Errorf("chunk %d: unexpected message extended, got %v, wanted %v", index, chunk.message.extended, expectedChunk.message.extended)
	}
	if chunk.message.timestamp != expectedChunk.message.timestamp {
		t.Errorf("chunk %d: unexpected message timestamp, got %v, wanted %v", index, chunk.message.timestamp, expectedChunk.message.timestamp)
	}
	if bytes.Compare(chunk.message.data, expectedChunk.message.data) != 0 {
		t.Errorf("chunk %d: unexpected message data, got '%v', wanted '%v'", index, chunk.message.data, expectedChunk.message.data)
	}
}

type chunkRecvTest struct {
	chunkSize int
	input     [][]byte
	chunks    []*rtmpChunk
}

var chunkRecvTests = []chunkRecvTest{
	{
		32,
		[][]byte{
			[]byte{0x03, 0x00, 0x0B, 0x68, 0x00, 0x00, 0x19, 0x14, 0x01, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
		},
		[]*rtmpChunk{
			&rtmpChunk{
				fmt:      0,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data: []byte{0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x74,
						0x72, 0x65, 0x61, 0x6D, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
				},
			},
		},
	},
	{
		16,
		[][]byte{
			[]byte{0x03, 0x00, 0x0B, 0x68, 0x00, 0x00, 0x19, 0x14, 0x01, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x74, 0x72,
				0x65, 0x61, 0x6D, 0x00},
			[]byte{0x43, 0x00, 0x00, 0x14, 0x00, 0x00, 0x19, 0x14,
				0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
		},
		[]*rtmpChunk{
			&rtmpChunk{
				fmt:      0,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data: []byte{0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x74,
						0x72, 0x65, 0x61, 0x6D, 0x00},
				},
			},
			&rtmpChunk{
				fmt:      1,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2940,
					data: []byte{0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x53, 0x74,
						0x72, 0x65, 0x61, 0x6D, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
				},
			},
		},
	},
	{
		8,
		[][]byte{
			[]byte{0x03, 0x00, 0x0B, 0x68, 0x00, 0x00, 0x19, 0x14, 0x01, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74},
			[]byte{0x43, 0x00, 0x00, 0x14, 0x00, 0x00, 0x19, 0x14,
				0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00},
			[]byte{0x83, 0x00, 0x00, 0x2a,
				0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			[]byte{0xC3,
				0x05},
		},
		[]*rtmpChunk{
			&rtmpChunk{
				fmt:      0,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data: []byte{
						0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74},
				},
			},
			&rtmpChunk{
				fmt:      1,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2940,
					data: []byte{
						0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74,
						0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00},
				},
			},
			&rtmpChunk{
				fmt:      2,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2982,
					data: []byte{
						0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74,
						0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00,
						0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				},
			},
			&rtmpChunk{
				fmt:      3,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   3024,
					data: []byte{
						0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74,
						0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00,
						0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
				},
			},
		},
	},
}

func TestRecvChunk(t *testing.T) {
	logFunc := func(format string, a ...interface{}) {
		t.Logf(format, a...)
	}
	for _, c := range chunkRecvTests {
		var prevMessage *rtmpMessage = nil
		for index, data := range c.input {
			buf := bytes.NewBuffer(data)
			chunk, err := recvChunk(buf, func(chunk *rtmpChunk) *rtmpMessage { return prevMessage }, c.chunkSize, logFunc)
			if err != nil {
				t.Error(err)
			}
			expectedChunk := c.chunks[index]
			testChunk(t, index, chunk, expectedChunk)
			prevMessage = chunk.message
		}
	}
}

type chunkChunkTest struct {
	chunkSize int
	streamID  int
	message   *rtmpMessage
	chunks    []*rtmpChunk
}

var chunkChunkTests = []chunkChunkTest{
	{
		8, 3,
		&rtmpMessage{
			length:      25,
			messageType: 0x14,
			streamID:    0x1,
			timestamp:   2920,
			data: []byte{
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74,
				0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00,
				0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
		},
		[]*rtmpChunk{
			&rtmpChunk{
				fmt:      0,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data:        []byte{0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74},
				},
			},
			&rtmpChunk{
				fmt:      3,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data:        []byte{0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00},
				},
			},
			&rtmpChunk{
				fmt:      3,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data:        []byte{0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				},
			},
			&rtmpChunk{
				fmt:      3,
				streamID: 3,
				message: &rtmpMessage{
					length:      25,
					messageType: 0x14,
					streamID:    0x1,
					timestamp:   2920,
					data:        []byte{0x05},
				},
			},
		},
	},
}

func TestChunkMessage(t *testing.T) {
	logFunc := func(format string, a ...interface{}) {
		t.Logf(format, a...)
	}
	for _, c := range chunkChunkTests {
		chunks := make([]*rtmpChunk, 0)
		chunkMessage(c.message, func() int { return c.streamID }, c.chunkSize, func(chunk *rtmpChunk) {
			chunks = append(chunks, chunk)
		}, logFunc)
		for index, chunk := range chunks {
			testChunk(t, index, chunk, c.chunks[index])
		}
	}
}

type chunkSendTest struct {
	chunkSize int
	streamID  int
	message   *rtmpMessage
	data      [][]byte
}

var chunkSendTests = []chunkSendTest{
	{
		8, 3,
		&rtmpMessage{
			length:      25,
			messageType: 0x14,
			streamID:    0x1,
			timestamp:   2920,
			data: []byte{
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74,
				0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00,
				0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
		},
		[][]byte{
			[]byte{0x03, 0x00, 0x0B, 0x68, 0x00, 0x00, 0x19, 0x14, 0x01, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x0C, 0x63, 0x72, 0x65, 0x61, 0x74},
			[]byte{0xC3,
				0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x00},
			[]byte{0xC3,
				0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			[]byte{0xC3,
				0x05},
		},
	},
}

func TestSendMessage(t *testing.T) {
	logFunc := func(format string, a ...interface{}) {
		t.Logf(format, a...)
	}
	for _, c := range chunkSendTests {
		chunks := make([]*rtmpChunk, 0)
		chunkMessage(c.message, func() int { return c.streamID }, c.chunkSize, func(chunk *rtmpChunk) {
			chunks = append(chunks, chunk)
		}, logFunc)
		for index, chunk := range chunks {
			buf := &bytes.Buffer{}
			err := sendChunk(buf, chunk, logFunc)
			if err != nil {
				t.Error(err)
			}
			if bytes.Compare(buf.Bytes(), c.data[index]) != 0 {
				t.Errorf("chunk %d: unexpected message data, got '%v', wanted '%v'", index, buf.Bytes(), c.data[index])
			}
		}
	}
}
