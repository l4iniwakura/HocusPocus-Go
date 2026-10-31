package hocuspocus

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"testing"
)

func TestTextAttributeInspectionFromOriginalYjs(t *testing.T) {
	raw, _ := base64.StdEncoding.DecodeString("AQT5hennBQAHAQtwcm9zZW1pcnJvcgMJcGFyYWdyYXBoBwD5hennBQAGBAD5hennBQEHdmlzaWJsZSgA+YXp5wUBB29uY2xpY2sBdwZoaWRkZW4A")
	d := crdt.New()
	if err := crdt.ApplyUpdateV1(d, raw, nil); err != nil {
		t.Fatal(err)
	}
	text := d.GetXmlFragment("prosemirror").Children()[0].(*crdt.YXmlElement).Children()[0].(*crdt.YXmlText)
	if text.ToString() != "visible" || !text.HasMapAttributes() {
		t.Fatal("wire attributes lost")
	}
}

func TestPublicMessageReceiverAppliesAndReplies(t *testing.T) {
	s, _ := testServer(t, nil)
	c, err := s.OpenDirect(context.Background(), "public-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(context.Background(), true)
	update := textUpdate("native Go")
	message, err := NewIncomingMessage(NewOutgoingMessage("public-api").CreateSyncMessage().WriteUpdate(update).Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err = (MessageReceiver{Message: message}).Apply(context.Background(), c.Document(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := c.Document().Doc.GetText("body").ToString(); got != "native Go" {
		t.Fatal(got)
	}
	sync := NewOutgoingMessage("public-api").CreateSyncMessage().WriteFirstSyncStepFor(c.Document()).Bytes()
	message, err = NewIncomingMessage(sync)
	if err != nil {
		t.Fatal(err)
	}
	var reply []byte
	if err = (MessageReceiver{Message: message}).Apply(context.Background(), c.Document(), nil, func(b []byte) error { reply = b; return nil }); err != nil {
		t.Fatal(err)
	}
	response, err := NewIncomingMessage(reply)
	if err != nil {
		t.Fatal(err)
	}
	step, _ := response.ReadVarUint()
	if step != 1 {
		t.Fatal(step)
	}
	peek, _ := response.PeekVarBytes()
	actual, _ := response.ReadVarBytes()
	if !bytes.Equal(peek, actual) {
		t.Fatal("peek advanced decoder")
	}
}
func TestRootInspectionPreservesSequence(t *testing.T) {
	d := crdt.New()
	d.Transact(func(tx *crdt.Transaction) { tx.GetText("wrong-map").Insert(tx, 0, "history", nil) })
	encoded := d.EncodeStateAsUpdate()
	copy := crdt.New()
	if err := crdt.ApplyUpdateV1(copy, encoded, nil); err != nil {
		t.Fatal(err)
	}
	if !copy.RootHasSequence("wrong-map") {
		t.Fatal("decoded root sequence hidden")
	}
	if copy.RootHasSequence("absent") {
		t.Fatal("absent root has sequence")
	}
}
