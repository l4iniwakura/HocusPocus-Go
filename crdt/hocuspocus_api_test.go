package crdt

import "testing"

func TestHocuspocusRootNamesOwnsSnapshot(t *testing.T) {
	doc := New()
	doc.GetText("z")
	doc.GetXmlFragment("a")
	names := doc.RootNames()
	if len(names) != 2 || names[0] != "a" || names[1] != "z" {
		t.Fatal(names)
	}
	names[0] = "changed"
	if doc.RootNames()[0] != "a" {
		t.Fatal("root names aliased")
	}
}
func TestHocuspocusGCFilter(t *testing.T) {
	for _, collect := range []bool{false, true} {
		calls := 0
		doc := New(WithClientID(17), WithGCFilter(func(*Item) bool { calls++; return collect }))
		doc.Transact(func(tx *Transaction) { tx.GetText("body").Insert(tx, 0, "retained", nil) })
		doc.Transact(func(tx *Transaction) { tx.GetText("body").Delete(tx, 0, 8) })
		if calls == 0 {
			t.Fatal("filter not called")
		}
		for _, item := range doc.store.clients[17] {
			_, deleted := item.Content.(*ContentDeleted)
			if item.Deleted && deleted != collect {
				t.Fatal("GC filter ignored")
			}
		}
		RunGC(doc)
		for _, item := range doc.store.clients[17] {
			_, deleted := item.Content.(*ContentDeleted)
			if item.Deleted && deleted != collect {
				t.Fatal("RunGC filter ignored")
			}
		}
	}
}
