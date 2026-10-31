package screens

import (
	"slices"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// TreeNode is one entry of a Tree: a folder when it has Children, else a leaf.
type TreeNode struct {
	Name     string
	Children []TreeNode
	Dot      kit.Role // leaf bullet colour
	Mark     string   // short flag at the far right, like the piece's "M"
	MarkRole kit.Role
	Info     string // muted detail before the mark, e.g. "6p · 120,431"
	Changed  bool   // leaf name drawn orange, as the piece draws modified files
}

// Tree is the file-tree piece: an explorer panel with a branch badge, a
// header line, folders that open with ▼ and leaves with coloured bullets.
// kaas topics --tree draws topic prefixes with it.
type Tree struct {
	Title     string // "TOPICS"
	Root      string // the folder everything hangs from, like the piece's repo
	Badge     string // purple, after ◆
	Head      string
	HeadRight string // orange, like "3 changes"
	Nodes     []TreeNode
	Empty     string
	Foot      string
	FootRight string
}

// Render draws the panel at width.
func (t Tree) Render(k kit.Kit, width int) []string {
	w := min(width, 90)
	inner := w - 2
	body := []string{" " + spread(k.S(kit.FG, t.Head), k.S(kit.Warn, t.HeadRight)+" ", inner-1), k.S(kit.Faint, strings.Repeat("─", inner))}
	var walk func(nodes []TreeNode, prefix string)
	walk = func(nodes []TreeNode, prefix string) {
		for i, n := range nodes {
			branch, next := "├─ ", "│  "
			if i == len(nodes)-1 {
				branch, next = "└─ ", "   "
			}
			lead := "  " + k.S(kit.Rule, prefix+branch)
			room := inner - 2 - kit.Width(prefix+branch) - 2
			if len(n.Children) > 0 {
				body = append(body, lead+k.S(kit.Muted, "▼ ")+k.S(kit.Accent, kit.Fit(n.Name+"/", room)))
				walk(n.Children, prefix+next)
				continue
			}
			right := strings.TrimSpace(k.S(kit.Muted, n.Info) + " " + k.S(n.MarkRole, n.Mark))
			name := n.Name
			if kit.Width(name)+kit.Width(right)+2 > room {
				right = k.S(n.MarkRole, n.Mark)
			}
			nameRole := kit.FG
			if n.Changed {
				nameRole = kit.Warn
			}
			label := k.S(n.Dot, "•") + " " + k.S(nameRole, kit.Fit(name, max(1, room-kit.Width(right)-3)))
			body = append(body, spread(lead+label, right+" ", inner))
		}
	}
	if t.Root != "" {
		body = append(body, "  "+k.S(kit.Muted, "▼ ")+k.S(kit.Accent, kit.Fit(t.Root+"/", inner-6)))
	}
	walk(t.Nodes, "")
	if len(t.Nodes) == 0 {
		for _, l := range kit.Wrap(t.Empty, inner-2) {
			body = append(body, " "+k.S(kit.Muted, l))
		}
	}
	body = append(body, "", k.S(kit.Faint, strings.Repeat("─", inner)))
	if kit.Width(t.Foot)+kit.Width(t.FootRight)+4 <= inner {
		body = append(body, " "+spread(k.S(kit.FG, t.Foot), k.S(kit.Muted, t.FootRight), inner-2))
	} else {
		for _, part := range []string{t.Foot, t.FootRight} {
			if part == "" {
				continue
			}
			for _, l := range kit.Wrap(part, inner-2) {
				body = append(body, " "+k.S(kit.Muted, l))
			}
		}
	}
	badge := ""
	if t.Badge != "" {
		badge = k.S(kit.Accent2, "◆ "+t.Badge)
	}
	return kit.Clip(width, k.Draw(kit.Box{Title: k.S(kit.Muted, t.Title), Right: badge, Role: kit.Rule, Width: w}, body))
}

// Nest groups names into a tree by splitting on sep, folders before leaves,
// each sorted by name. A folder that holds only one folder is merged into
// it ("payments.eu/"), so deep single chains don't waste rows. leaf builds
// each topic's node from its full name.
func Nest(names []string, sep string, leaf func(full string) TreeNode) []TreeNode {
	type dir struct {
		dirs   map[string]*dir
		leaves []TreeNode
	}
	root := &dir{dirs: map[string]*dir{}}
	for _, full := range names {
		parts := []string{full}
		if sep != "" {
			parts = strings.Split(full, sep)
		}
		d := root
		for _, p := range parts[:len(parts)-1] {
			if d.dirs[p] == nil {
				d.dirs[p] = &dir{dirs: map[string]*dir{}}
			}
			d = d.dirs[p]
		}
		n := leaf(full)
		n.Name = parts[len(parts)-1]
		d.leaves = append(d.leaves, n)
	}
	var build func(d *dir) []TreeNode
	build = func(d *dir) []TreeNode {
		var out []TreeNode
		names := make([]string, 0, len(d.dirs))
		for name := range d.dirs {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			sub := d.dirs[name]
			label := name
			for len(sub.leaves) == 0 && len(sub.dirs) == 1 {
				for only, next := range sub.dirs {
					label, sub = label+sep+only, next
				}
			}
			out = append(out, TreeNode{Name: label, Children: build(sub)})
		}
		leaves := append([]TreeNode(nil), d.leaves...)
		slices.SortFunc(leaves, func(a, b TreeNode) int { return strings.Compare(a.Name, b.Name) })
		return append(out, leaves...)
	}
	return build(root)
}
