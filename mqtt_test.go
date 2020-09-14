package mqtt

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	gbt "github.com/huin/gobinarytest"
)

type fakeSizePayload int

func (p fakeSizePayload) Size() int {
	return int(p)
}

func (p fakeSizePayload) WritePayload(w io.Writer) error {
	return nil
}

func (p fakeSizePayload) ReadPayload(r io.Reader) error {
	return nil
}

type fakeDecoderConfig struct{}

func (c fakeDecoderConfig) MakePayload(msg *Publish, r io.Reader, n int) (Payload, error) {
	return fakeSizePayload(n), nil
}

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		Comment       string
		DecoderConfig DecoderConfig
		Msg           Message
		Expected      gbt.Matcher
		NoEncodeTest  bool
	}{
		{
			Comment: "CONNECT message",
			Msg: &Connect{
				ProtocolName:    "MQIsdp",
				ProtocolVersion: 3,
				UsernameFlag:    true,
				PasswordFlag:    true,
				WillRetain:      false,
				WillQos:         1,
				WillFlag:        true,
				CleanSession:    true,
				KeepAliveTimer:  10,
				ClientId:        "xixihaha",
				WillTopic:       "topic",
				WillMessage:     "message",
				Username:        "name",
				Password:        "pwd",
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x10}},
				gbt.Named{"Remaining length", gbt.Literal{12 + 5*2 + 8 + 5 + 7 + 4 + 3}},

				// Extended headers for CONNECT:
				gbt.Named{"Protocol name", gbt.InOrder{gbt.Literal{0x00, 0x06}, gbt.Literal("MQIsdp")}},
				gbt.Named{
					"Extended headers for CONNECT",
					gbt.Literal{
						0x03,       // Protocol version number
						0xce,       // Connect flags
						0x00, 0x0a, // Keep alive timer
					},
				},

				// CONNECT payload:
				gbt.Named{"Client identifier", gbt.InOrder{gbt.Literal{0x00, 0x08}, gbt.Literal("xixihaha")}},
				gbt.Named{"Will topic", gbt.InOrder{gbt.Literal{0x00, 0x05}, gbt.Literal("topic")}},
				gbt.Named{"Will message", gbt.InOrder{gbt.Literal{0x00, 0x07}, gbt.Literal("message")}},
				gbt.Named{"Username", gbt.InOrder{gbt.Literal{0x00, 0x04}, gbt.Literal("name")}},
				gbt.Named{"Password", gbt.InOrder{gbt.Literal{0x00, 0x03}, gbt.Literal("pwd")}},
			},
		},

		{
			Comment: "CONNACK message",
			Msg: &ConnAck{
				SessionPresent:true,
				ReturnCode: RetCodeBadUsernameOrPassword,
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x20}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				// Session Present: bit 0 of the Connect Acknowledge Flags
				gbt.Named{"Connect Acknowledge Flags", gbt.Literal{1}},
				gbt.Named{"Return code", gbt.Literal{4}},
			},
		},

		{
			Comment: "PUBLISH message with QoS = QosAtMostOnce",
			Msg: &Publish{
				Header: Header{
					DupFlag:  false,
					QosLevel: QosAtMostOnce,
					Retain:   false,
				},
				TopicName: "a/b",
				Payload:   BytesPayload{1, 2, 3},
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x30}},
				gbt.Named{"Remaining length", gbt.Literal{5 + 3}},

				gbt.Named{"Topic", gbt.Literal{0x00, 0x03, 'a', '/', 'b'}},
				// No MessageId should be present.
				gbt.Named{"Data", gbt.Literal{1, 2, 3}},
			},
		},

		{
			Comment: "PUBLISH message with QoS = QosAtLeastOnce",
			Msg: &Publish{
				Header: Header{
					DupFlag:  true,
					QosLevel: QosAtLeastOnce,
					Retain:   false,
				},
				TopicName: "a/b",
				MessageId: 0x1234,
				Payload:   BytesPayload{1, 2, 3},
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x3a}},
				gbt.Named{"Remaining length", gbt.Literal{7 + 3}},

				gbt.Named{"Topic", gbt.Literal{0x00, 0x03, 'a', '/', 'b'}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
				gbt.Named{"Data", gbt.Literal{1, 2, 3}},
			},
		},

		{
			Comment:       "PUBLISH message with maximum size payload",
			DecoderConfig: fakeDecoderConfig{},
			Msg: &Publish{
				Header: Header{
					DupFlag:  false,
					QosLevel: QosAtMostOnce,
					Retain:   false,
				},
				TopicName: "a/b",
				Payload:   fakeSizePayload(MaxPayloadSize - 5),
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x30}},
				gbt.Named{"Remaining length", gbt.Literal{0xff, 0xff, 0xff, 0x7f}},

				gbt.Named{"Topic", gbt.Literal{0x00, 0x03, 'a', '/', 'b'}},
				// Our fake payload doesn't write any data, so no data should appear here.
			},
		},

		{
			Comment: "PUBACK message",
			Msg:     &PubAck{MessageId: 0x1234},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x40}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
			},
		},

		{
			Comment: "PUBREC message",
			Msg:     &PubRec{MessageId: 0x1234},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x50}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
			},
		},

		{
			Comment: "PUBREL message",
			Msg:     &PubRel{MessageId: 0x1234},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x60}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
			},
		},

		{
			Comment: "PUBCOMP message",
			Msg:     &PubComp{MessageId: 0x1234},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x70}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
			},
		},

		{
			Comment: "SUBSCRIBE message",
			Msg: &Subscribe{
				Header: Header{
					DupFlag:  false,
					QosLevel: QosAtLeastOnce,
				},
				MessageId: 0x4321,
				Topics: []TopicQos{
					{"a/b", QosAtLeastOnce},
					{"c/d", QosExactlyOnce},
				},
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x82}},
				gbt.Named{"Remaining length", gbt.Literal{2 + 5 + 1 + 5 + 1}},

				gbt.Named{"MessageId", gbt.Literal{0x43, 0x21}},
				gbt.Named{"First topic", gbt.Literal{0x00, 0x03, 'a', '/', 'b'}},
				gbt.Named{"First topic QoS", gbt.Literal{1}},
				gbt.Named{"Second topic", gbt.Literal{0x00, 0x03, 'c', '/', 'd'}},
				gbt.Named{"Second topic QoS", gbt.Literal{2}},
			},
		},

		{
			Comment: "SUBACK message",
			Msg: &SubAck{
				MessageId: 0x1234,
				TopicsQos: []QosLevel{QosAtMostOnce, QosExactlyOnce},
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x90}},
				gbt.Named{"Remaining length", gbt.Literal{4}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
				gbt.Named{"TopicsQos", gbt.Literal{0x00, 0x02}},
			},
		},

		{
			Comment: "UNSUBSCRIBE message",
			Msg: &Unsubscribe{
				Header: Header{
					DupFlag:  false,
					QosLevel: QosAtLeastOnce,
				},
				MessageId: 0x4321,
				Topics:    []string{"a/b", "c/d"},
			},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0xa2}},
				gbt.Named{"Remaining length", gbt.Literal{2 + 5 + 5}},

				gbt.Named{"MessageId", gbt.Literal{0x43, 0x21}},
				gbt.Named{"First topic", gbt.Literal{0x00, 0x03, 'a', '/', 'b'}},
				gbt.Named{"Second topic", gbt.Literal{0x00, 0x03, 'c', '/', 'd'}},
			},
		},

		{
			Comment: "UNSUBACK message",
			Msg:     &UnsubAck{MessageId: 0x1234},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0xb0}},
				gbt.Named{"Remaining length", gbt.Literal{2}},
				gbt.Named{"MessageId", gbt.Literal{0x12, 0x34}},
			},
		},

		{
			Comment: "PINGREQ message",
			Msg:     &PingReq{},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0xc0}},
				gbt.Named{"Remaining length", gbt.Literal{0}},
			},
		},

		{
			Comment: "PINGRESP message",
			Msg:     &PingResp{},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0xd0}},
				gbt.Named{"Remaining length", gbt.Literal{0}},
			},
		},

		{
			Comment: "DISCONNECT message",
			Msg:     &Disconnect{},
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0xe0}},
				gbt.Named{"Remaining length", gbt.Literal{0}},
			},
		},
	}

	for _, test := range tests {
		{
			// Test decoding.
			expectedBuf := new(bytes.Buffer)
			test.Expected.Write(expectedBuf)

			if decodedMsg, err := DecodeOneMessage(expectedBuf, test.DecoderConfig); err != nil {
				t.Errorf("%s: Unexpected error during decoding: %v", test.Comment, err)
			} else if !reflect.DeepEqual(test.Msg, decodedMsg) {
				t.Errorf("%s: Decoded value mismatch\n     got = %#v\nexpected = %#v",
					test.Comment, decodedMsg, test.Msg)
			}
		}

		if !test.NoEncodeTest {
			// Test encoding.
			encodedBuf := new(bytes.Buffer)
			if err := test.Msg.Encode(encodedBuf); err != nil {
				t.Errorf("%s: Unexpected error during encoding: %v", test.Comment, err)
			} else if err = gbt.Matches(test.Expected, encodedBuf.Bytes()); err != nil {
				t.Errorf("%s: Unexpected encoding output: %v", test.Comment, err)
			}
		}
	}
}

func TestErrorEncode(t *testing.T) {
	tests := []struct {
		Comment string
		Msg     Message
	}{
		{
			Comment: "Payload reports Size() that's too large for MQTT payload.",
			Msg: &Publish{
				TopicName: "big/message",
				MessageId: 0x1234,
				// MaxPayloadSize-5 is too large - the payload space is further
				// restricted by the variable header.
				Payload: fakeSizePayload(MaxPayloadSize),
			},
		},
		{
			Comment: "Payload reports Size() that would overflow when added to variable header size.",
			Msg: &Publish{
				TopicName: "big/message",
				MessageId: 0x1234,
				Payload:   fakeSizePayload(0x7fffffff),
			},
		},
	}

	for _, test := range tests {
		encodedBuf := new(bytes.Buffer)
		if err := test.Msg.Encode(encodedBuf); err == nil {
			t.Errorf("%s: Expected error during encoding, but got nil.", test.Comment)
		}
	}
}

func TestErrorDecode(t *testing.T) {
	tests := []struct {
		Comment  string
		Expected gbt.Matcher
	}{
		{
			Comment:  "Immediate EOF",
			Expected: gbt.Literal{},
		},
		{
			Comment:  "EOF at 1 byte",
			Expected: gbt.Literal{0x10},
		},
		{
			Comment: "PUBACK message with too short a length",
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x40}},
				gbt.Named{"Remaining length", gbt.Literal{1}},

				gbt.Named{"Truncated MessageId", gbt.Literal{0x12}},
			},
		},
		{
			Comment: "PUBACK message with too long a length",
			Expected: gbt.InOrder{
				gbt.Named{"Header byte", gbt.Literal{0x40}},
				gbt.Named{"Remaining length", gbt.Literal{3}},

				gbt.Named{"Truncated MessageId", gbt.Literal{0x12, 0x34, 0x56}},
			},
		},
	}

	for _, test := range tests {
		expectedBuf := new(bytes.Buffer)
		test.Expected.Write(expectedBuf)

		if _, err := DecodeOneMessage(expectedBuf, nil); err == nil {
			t.Errorf("%s: Expected error during decoding, but got nil.", test.Comment)
		}
	}
}

func TestLengthEncodeDecode(t *testing.T) {
	tests := []struct {
		Value   int32
		Encoded gbt.Matcher
	}{
		{0, gbt.Literal{0}},
		{1, gbt.Literal{1}},
		{20, gbt.Literal{20}},

		// Boundary conditions used as tests taken from MQTT 3.1 spec.
		{0, gbt.Literal{0x00}},
		{127, gbt.Literal{0x7F}},
		{128, gbt.Literal{0x80, 0x01}},
		{16383, gbt.Literal{0xFF, 0x7F}},
		{16384, gbt.Literal{0x80, 0x80, 0x01}},
		{2097151, gbt.Literal{0xFF, 0xFF, 0x7F}},
		{2097152, gbt.Literal{0x80, 0x80, 0x80, 0x01}},
		{268435455, gbt.Literal{0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, test := range tests {
		{
			// Test decoding.
			buf := new(bytes.Buffer)
			test.Encoded.Write(buf)
			buf = bytes.NewBuffer(buf.Bytes())
			if result := decodeLength(buf); test.Value != result {
				t.Errorf("Decoding test %#x: got %#x", test.Value, result)
			}
		}
		{
			// Test encoding.
			buf := new(bytes.Buffer)
			encodeLength(test.Value, buf)
			if err := gbt.Matches(test.Encoded, buf.Bytes()); err != nil {
				t.Errorf("Encoding test %#x: %v", test.Value, err)
			}
		}
	}
}

type SeqBytePayload struct {
	N int
	T *testing.T
}

func (p *SeqBytePayload) Size() int {
	return p.N
}

func (p *SeqBytePayload) WritePayload(w io.Writer) error {
	buf := make([]byte, 256)
	for i := range buf {
		buf[i] = byte(i)
	}

	for toWrite := p.N; toWrite > 0; toWrite -= 256 {
		writeThisTime := toWrite
		if writeThisTime > 256 {
			writeThisTime = 256
		}
		if _, err := w.Write(buf[:writeThisTime]); err != nil {
			return err
		}
	}
	return nil
}

func (p *SeqBytePayload) ReadPayload(r io.Reader) error {
	buf := make([]byte, 256)

	for toRead := p.N; toRead > 0; toRead -= 256 {
		readThisTime := toRead
		if readThisTime > 256 {
			readThisTime = 256
		}
		if _, err := io.ReadFull(r, buf[:readThisTime]); err != nil {
			p.T.Errorf("Got unexpected error %v", err)
			return err
		}
		for i, v := range buf[:readThisTime] {
			if v != byte(i) {
				p.T.Errorf("Got unexpected byte %#x, expected %#x", v, i)
				return io.ErrClosedPipe
			}
		}
	}

	// Any more reads should produce an EOF.
	if n, err := r.Read(buf); n > 0 {
		p.T.Errorf("Got unexpected extra %d bytes of data", n)
	} else if err != io.EOF {
		p.T.Errorf("Expected err=EOF, got %v", err)
	}

	return nil
}

// Stream a reasonably large Publish payload containing sequential byte values.
func TestPipedPublish(t *testing.T) {
	r, w := io.Pipe()

	complete := make(chan bool)

	payload := &SeqBytePayload{
		N: 20 + (1 << 20),
		T: t,
	}

	go func() {
		defer func() {
			complete <- true
			w.Close()
		}()

		msg := &Publish{
			TopicName: "foo",
			Payload:   payload,
		}
		if err := msg.Encode(w); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		defer func() {
			complete <- true
			r.Close()
		}()

		expectedMsg := &Publish{
			TopicName: "foo",
			Payload:   payload,
		}

		testConfig := &ValueConfig{payload}

		if msg, err := DecodeOneMessage(r, testConfig); err != nil {
			t.Error(err)
		} else if !reflect.DeepEqual(expectedMsg, msg) {
			t.Errorf("     got = %#v\nexpected = %#v", msg, expectedMsg)
		}
	}()

	_ = <-complete
	_ = <-complete
}
