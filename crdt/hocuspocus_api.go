package crdt

import "sort"

// RootNames returns a sorted copy of named root types, including empty roots.
// Like other document readers it must not be called inside Transact.
func (d *Doc) RootNames() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	names := make([]string, 0, len(d.share))
	for name := range d.share {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RootTypeName distinguishes materialized root types without changing them.
// A root decoded from the wire may still be untyped ("unknown").
func (d *Doc) RootTypeName(name string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	switch d.share[name].(type) {
	case nil:
		return "absent"
	case *YXmlFragment:
		return "xml"
	case *YText:
		return "text"
	case *YMap:
		return "map"
	case *YArray:
		return "array"
	default:
		return "unknown"
	}
}

// RootIsEmpty inspects a root without materializing it as a different type.
func (d *Doc) RootIsEmpty(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	root := d.share[name]
	if root == nil {
		return true
	}
	base := root.baseType()
	return base.start == nil && len(base.itemMap) == 0
}

// WithGCFilter preserves deleted items for which filter returns false.
// The filter runs synchronously under the document lock and must not call Doc
// methods or mutate the item. Nil collects all eligible deleted items.
func WithGCFilter(filter func(*Item) bool) DocOption { return func(d *Doc) { d.gcFilter = filter } }

// RootHasSequence detects sequence history before materializing an untyped root
// as a map. This prevents a wrong-type accessor from hiding invalid wire data.
func (d *Doc) RootHasSequence(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	root := d.share[name]
	return root != nil && root.baseType().start != nil
}

// HasMapAttributes reports live attributes on a Y.Text/Y.XmlText object. These
// are distinct from inline formatting and are not represented in ProseMirror JSON.
func (t *YText) HasMapAttributes() bool {
	if t.doc != nil {
		t.doc.mu.RLock()
		defer t.doc.mu.RUnlock()
	}
	for _, item := range t.itemMap {
		if !item.Deleted {
			return true
		}
	}
	return false
}
