package broadcast

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"time"
)

const protocolVersion byte = 3
const defaultChunkSize int = 128

type LogFunc func(format string, a ...interface{})

type RTMPClient struct {
	log           LogFunc
	url           *url.URL
	conn          net.Conn
	chunkSizeRecv int
	chunkSizeSend int
}

type rtmpMessage struct {
	streamID       int
	messageType    byte
	length         int
	extended       bool
	timestamp      int64
	timestampDelta int
	data           []byte
}

func (m *rtmpMessage) copyFrom(other *rtmpMessage) {
	m.streamID = other.streamID
	m.messageType = other.messageType
	m.length = other.length
	m.extended = other.extended
	m.timestamp = other.timestamp
	m.timestampDelta = other.timestampDelta
}

func (m *rtmpMessage) copyFromWithData(other *rtmpMessage) {
	m.copyFrom(other)
	m.data = make([]byte, len(other.data))
	copy(m.data, other.data)
}

type rtmpChunk struct {
	fmt      byte
	streamID int
	message  *rtmpMessage
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func NewRTMPClient(u string, log LogFunc) (*RTMPClient, error) {
	var err error
	c := &RTMPClient{
		log: log,
		chunkSizeRecv: defaultChunkSize,
		chunkSizeSend: defaultChunkSize,
	}
	c.url, err = url.Parse(u)
	c.log("Handshake: %s", u)
	if err != nil {
		return nil, err
	}
	c.log("Handshake: Host: %s", c.url.Host)
	c.conn, err = net.DialTimeout("tcp", c.url.Host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *RTMPClient) Handshake() error {
	// C0
	c.conn.Write([]byte{protocolVersion})

	// S0
	version := make([]byte, 1)
	if _, err := c.conn.Read(version); err != nil {
		return err
	}
	c.log("Handshake: S0 version=%d", version[0])
	if version[0] != protocolVersion {
		return fmt.Errorf("S0 version (%d) != C0 version (%d)", version[0], protocolVersion)
	}

	// C1
	timeC1 := time.Now()
	binary.Write(c.conn, binary.BigEndian, uint32(0)) // time, 0 for C1
	binary.Write(c.conn, binary.BigEndian, uint32(0)) // zero, always zero
	rand1 := randBytes(1528)
	c.conn.Write(rand1)
	c.log("Handshake: C1 rand %#v", rand1[:20])

	// S1
	S1 := make([]byte, 1536)
	if _, err := c.conn.Read(S1); err != nil {
		return err
	}
	timeS1 := time.Now()
	c.log("Handshake: S1 time %#v", S1[0:4])
	c.log("Handshake: S1 zero %#v", S1[4:8])
	c.log("Handshake: S1 rand %#v", S1[8:28])

	// C2
	c.conn.Write(S1[0:4])
	timestampC2 := timeS1.Sub(timeC1).Nanoseconds() / 1000
	binary.Write(c.conn, binary.BigEndian, uint32(timestampC2)) // time at which S1 was read
	c.conn.Write(S1[8:])

	// S2
	S2 := make([]byte, 1536)
	if _, err := c.conn.Read(S2); err != nil {
		return err
	}
	c.log("Handshake: S2 time %#v", S2[0:4])
	c.log("Handshake: S2 time2 %#v", S2[4:8])
	c.log("Handshake: S2 rand %#v", S2[8:28])

	if bytes.Compare(rand1, S2[8:]) != 0 {
		c.log("Handshake: S2 sent back mismatching bytes (from C1)")
		return fmt.Errorf("S2 random mismatch from C1")
	}

	c.log("Handshake: done")
	return nil
}

type prevMessageFunc func(*rtmpChunk) *rtmpMessage

func recvChunk(r io.Reader, prevMessageFunc prevMessageFunc, chunkSize int, log LogFunc) (*rtmpChunk, error) {
	header := make([]byte, 16)
	if _, err := r.Read(header[:1]); err != nil {
		return nil, fmt.Errorf("can't read basic header: %s", err)
	}

	chunk := &rtmpChunk{}
	chunk.fmt = header[0] >> 6
	chunk.streamID = int(header[0] & 0x3f)
	switch chunk.streamID {
	case 0:
		if _, err := r.Read(header[:1]); err != nil {
			return nil, fmt.Errorf("can't read chunk stream id byte 0: %s", err)
		}
		chunk.streamID = int(header[0] + 64)
	case 1:
		if _, err := r.Read(header[:1]); err != nil {
			return nil, fmt.Errorf("can't read chunk stream id byte 1: %s", err)
		}
		chunk.streamID = int(header[0] + 64)
		if _, err := r.Read(header[:1]); err != nil {
			return nil, fmt.Errorf("can't read chunk stream id byte 2: %s", err)
		}
		chunk.streamID = chunk.streamID + int(header[0]<<8)
	}

	// always copy previous message, overwrite later
	prevMessage := prevMessageFunc(chunk)
	chunk.message = &rtmpMessage{}
	if prevMessage != nil {
		chunk.message.copyFromWithData(prevMessage)
	}

	var timestamp int
	if chunk.fmt == 0 || chunk.fmt == 1 || chunk.fmt == 2 {
		header[0] = 0
		if _, err := r.Read(header[1:4]); err != nil {
			return nil, fmt.Errorf("can't read chunk timestamp: %s", err)
		}
		timestamp = int(binary.BigEndian.Uint32(header[:4]))
		if timestamp == 0xffffff {
			chunk.message.extended = true
		}
	}
	if chunk.fmt == 0 || chunk.fmt == 1 {
		header[0] = 0
		if _, err := r.Read(header[1:4]); err != nil {
			return nil, fmt.Errorf("can't read message length: %s", err)
		}
		chunk.message.length = int(binary.BigEndian.Uint32(header[:4]))
		header[0] = 0
		if _, err := r.Read(header[:1]); err != nil {
			return nil, fmt.Errorf("can't read message type: %s", err)
		}
		chunk.message.messageType = header[0]
	}
	if chunk.fmt == 0 {
		if _, err := r.Read(header[:4]); err != nil {
			return nil, fmt.Errorf("can't read message stream id: %s", err)
		}
		// little-endian as per spec
		chunk.message.streamID = int(binary.LittleEndian.Uint32(header[:4]))
	}

	// extended timestamp
	if chunk.message.extended {
		if _, err := r.Read(header[:4]); err != nil {
			return nil, fmt.Errorf("can't read message stream id: %s", err)
		}
		timestamp = int(binary.BigEndian.Uint32(header[:4]))
	}

	// type 3 uses the previous delta
	if chunk.fmt == 3 {
		timestamp = chunk.message.timestampDelta
	}
	if chunk.fmt == 0 {
		// type 0 has absolute timestamp
		chunk.message.timestampDelta = 0
		chunk.message.timestamp = int64(timestamp)
	} else {
		chunk.message.timestampDelta = timestamp
		chunk.message.timestamp += int64(timestamp)
	}

	// read message payload
	lenRemaining := chunk.message.length - len(chunk.message.data)
	lenToRead := lenRemaining
	if lenToRead > chunkSize {
		lenToRead = chunkSize
	}
	newData := make([]byte, lenToRead)
	if _, err := r.Read(newData); err != nil {
		return nil, fmt.Errorf("can't read message payload: %s", err)
	}
	chunk.message.data = append(chunk.message.data, newData...)

	return chunk, nil
}

type chunkFunc func(*rtmpChunk)
type chunkStreamIDFunc func() int

func chunkMessage(message *rtmpMessage, chunkStreamIDFunc chunkStreamIDFunc, chunkSize int, chunkFunc chunkFunc, log LogFunc) {
	startOffset := 0
	newStreamID := chunkStreamIDFunc()
	for startOffset < len(message.data) {
		chunk := &rtmpChunk{
			fmt:      0,
			streamID: newStreamID,
			message:  &rtmpMessage{},
		}
		if startOffset > 0 {
			chunk.fmt = 3
		}
		chunk.message.copyFrom(message)
		lenToSend := len(message.data) - startOffset
		if lenToSend > chunkSize {
			lenToSend = chunkSize
		}
		chunk.message.data = make([]byte, lenToSend)
		copy(chunk.message.data, message.data[startOffset:startOffset+lenToSend])
		startOffset += lenToSend
		chunkFunc(chunk)
	}
}

func sendChunk(w io.Writer, chunk *rtmpChunk, log LogFunc) error {
	if chunk.streamID == 0 || chunk.streamID == 1 {
		return fmt.Errorf("streamID 0 or 1 are reserved")
	}

	if chunk.streamID < 64 {
		_, err := w.Write([]byte{
			byte(chunk.fmt<<6 | byte(chunk.streamID))})
		if err != nil {
			return err
		}
	} else if chunk.streamID < 320 {
		_, err := w.Write([]byte{
			byte(chunk.fmt << 6),
			byte(chunk.streamID - 64)})
		if err != nil {
			return err
		}
	} else if chunk.streamID < 65600 {
		_, err := w.Write([]byte{
			byte(chunk.fmt<<6 | 1),
			byte(chunk.streamID - 64),
			byte((chunk.streamID - 64) >> 8)})
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("streamID %d is out of range", chunk.streamID)
	}

	timestamp := chunk.message.timestamp
	if chunk.fmt != 0 {
		timestamp = int64(chunk.message.timestampDelta)
	}

	buf := make([]byte, 4)
	if chunk.fmt == 0 || chunk.fmt == 1 || chunk.fmt == 2 {
		if timestamp > 0xffffff {
			if !chunk.message.extended {
				return fmt.Errorf("extended timestamp but extended is false")
			}
			binary.BigEndian.PutUint32(buf, 0xffffff)
		} else {
			binary.BigEndian.PutUint32(buf, uint32(timestamp))
		}
		w.Write(buf[1:])
	}
	if chunk.fmt == 0 || chunk.fmt == 1 {
		binary.BigEndian.PutUint32(buf, uint32(chunk.message.length))
		w.Write(buf[1:])
		w.Write([]byte{chunk.message.messageType})
	}
	if chunk.fmt == 0 {
		binary.LittleEndian.PutUint32(buf, uint32(chunk.message.streamID))
		w.Write(buf)
	}

	if chunk.message.extended {
		if timestamp <= 0xffffff {
			return fmt.Errorf("extended timestamp but timestamp fits in 3 bytes")
		}
		binary.BigEndian.PutUint32(buf, uint32(timestamp))
		w.Write(buf)
	}

	w.Write(chunk.message.data)

	return nil
}
