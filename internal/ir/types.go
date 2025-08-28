package ir

import (
	"fmt"
	"strings"
	"github.com/cespare/xxhash/v2"
)

type PrimitiveID uint32
type RuleID uint32

// Primitive: biểu diễn một điều kiện đơn giản (một primitive) trong rule
// Ví dụ: field="process.name", matchType="equals", values=["cmd.exe"], modifiers=["nocase"]
type Primitive struct {
	Field     string   `json:"field"`
	MatchType string   `json:"match_type"`
	Values    []string `json:"values"`
	Modifiers []string `json:"modifiers"`
}

// NewPrimitive: tạo một Primitive mới, có copy dữ liệu để tránh bị thay đổi ngoài ý muốn
func NewPrimitive(field, matchType string, values, modifiers []string) *Primitive {
    return &Primitive{
        Field:     field,
        MatchType: matchType,
        Values:    copyStrings(values),
        Modifiers: copyStrings(modifiers),
    }
}

// NewStaticPrimitive: tạo Primitive từ các literal (string cố định trong code)
func NewStaticPrimitive(field, matchType string, values, modifiers []string) *Primitive {
    return NewPrimitive(field, matchType, values, modifiers)
}

// FromStrings: tạo Primitive từ các slice string (giống NewPrimitive, chỉ khác ngữ nghĩa)
func FromStrings(field, matchType string, values, modifiers []string) *Primitive {
    return NewPrimitive(field, matchType, values, modifiers)
}

// copyStrings: tạo một bản sao của slice string để đảm bảo dữ liệu không bị thay đổi ngoài ý muốn
func copyStrings(src []string) []string {
	if src == nil {
		return nil
	}
	tmp := make([]string, len(src))
	copy(tmp, src)
	return tmp
}

// String: chuyển Primitive thành chuỗi để debug/log
func (p *Primitive) String() string {
	return fmt.Sprintf("Field: %s, MatchType: %s, Values: %v, Modifiers: %v",
		p.Field, p.MatchType, p.Values, p.Modifiers)
}

// Equal: so sánh 2 Primitive xem có giống hệt nhau không (so sánh cả field, matchType, values, modifiers)
func (p *Primitive) Equal(other *Primitive) bool {
    if other == nil {
        return false
    }
    return p.Field == other.Field &&
           p.MatchType == other.MatchType &&
           stringSlicesEqual(p.Values, other.Values) &&
           stringSlicesEqual(p.Modifiers, other.Modifiers)
}

// stringSlicesEqual: so sánh 2 slice string theo thứ tự phần tử
func stringSlicesEqual(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}

// Clone: tạo một bản sao mới của Primitive (deep copy)
func (p *Primitive) Clone() *Primitive {
    return NewPrimitive(p.Field, p.MatchType, p.Values, p.Modifiers)
}

// Hash: tạo ra giá trị băm (hash) duy nhất cho Primitive
// Dùng xxhash để so sánh/tìm kiếm nhanh Primitive trong map
func (p *Primitive) Hash() uint64 {
    h := xxhash.New()  

    h.Write([]byte(p.Field))
    h.Write([]byte(p.MatchType))
    h.Write([]byte(strings.Join(p.Values, "|")))    
    h.Write([]byte(strings.Join(p.Modifiers, "|")))

    return h.Sum64()
}

// CompiledRuleset: tập hợp các Primitive đã được biên dịch
// Lưu map từ key -> ID và danh sách các Primitive
type CompiledRuleset struct {
    PrimitiveMap  map[string]PrimitiveID `json:"primitive_map"` // ánh xạ primitive key sang ID
    Primitives    []Primitive            `json:"primitives"`    // danh sách primitive
    primitiveKeys map[string]string      // lưu lại key đã sinh
}

// NewCompiledRuleset: tạo ruleset rỗng
func NewCompiledRuleset() *CompiledRuleset {
    return &CompiledRuleset{
        PrimitiveMap:  make(map[string]PrimitiveID),
        Primitives:    make([]Primitive, 0),
        primitiveKeys: make(map[string]string),
    }
}

// PrimitiveCount: trả về số lượng primitive trong ruleset
func (cr *CompiledRuleset) PrimitiveCount() int {
    return len(cr.Primitives)
}

// GetPrimitive: lấy Primitive theo ID (nếu có)
func (cr *CompiledRuleset) GetPrimitive(id PrimitiveID) (*Primitive, bool) {
    if int(id) >= len(cr.Primitives) {
        return nil, false
    }
    return &cr.Primitives[id], true
}

// AddPrimitive: thêm một primitive mới vào ruleset
// Nếu primitive đã tồn tại thì trả về ID cũ, nếu chưa có thì thêm mới và trả về ID mới
func (cr *CompiledRuleset) AddPrimitive(primitive Primitive) PrimitiveID {
    key := cr.primitiveToKey(&primitive)
    
    if id, exists := cr.PrimitiveMap[key]; exists {
        return id
    }
    
    id := PrimitiveID(len(cr.Primitives))
    cr.Primitives = append(cr.Primitives, primitive)
    cr.PrimitiveMap[key] = id
    cr.primitiveKeys[key] = key
    
    return id
}

// primitiveToKey: sinh ra khóa duy nhất cho một primitive dựa trên field, matchType, values, modifiers
func (cr *CompiledRuleset) primitiveToKey(p *Primitive) string {
    var parts []string
    parts = append(parts, p.Field)
    parts = append(parts, p.MatchType)
    parts = append(parts, strings.Join(p.Values, "|"))
    parts = append(parts, strings.Join(p.Modifiers, "|"))
    return strings.Join(parts, "::")
}

// Clone: tạo bản sao của ruleset (deep copy toàn bộ primitive)
func (cr *CompiledRuleset) Clone() *CompiledRuleset {
    newRuleset := NewCompiledRuleset()
    
    for _, primitive := range cr.Primitives {
        newRuleset.AddPrimitive(*primitive.Clone())
    }
    
    return newRuleset
}
