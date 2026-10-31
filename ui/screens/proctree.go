package screens

import (
	"slices"
	"strings"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

// ProcNode is one row of a ProcTree and the rows that hang from it.
type ProcNode struct {
	ID       string // left column, like the piece's PID; may be blank
	Cells    []Cell // one per ProcTree.Columns; the zero Role is white
	Name     string
	NameRole kit.Role
	Args     string // muted text after the name, dropped when there is no room
	Tags     []Cell // at the far right, like the piece's "+new" and "exit 0"
	Children []ProcNode
}

// Legend is one "● label" entry in the footer.
type Legend struct {
	Label string
	Role  kit.Role
}

// ProcTree is the process-tree piece: a ptree listing with a PID column,
// load columns coloured by how busy each row is, and a command column whose
// branch lines show what runs under what. kaas brokers draws the cluster,
// its brokers and the topics they lead with it.
type ProcTree struct {
	Title     string // in the top edge, muted
	IDName    string // header of the ID column, "ID"
	Columns   []Column
	TreeName  string // header of the tree column, "NODE"
	Count     string // blue, at the right of the header: "3 brokers"
	Root      ProcNode
	Load      string // footer figure, like the piece's "load 1.42"
	LoadValue string
	LoadRole  kit.Role
	Legend    []Legend
	FootRight string
}

// Render draws the tree at width. Narrow frames drop the highest-Drop
// columns, then a row's args, then its tags, before cutting a name.
func (p ProcTree) Render(k kit.Kit, width int) []string {
	w := min(width, 100)
	inner := w - 2

	idW := kit.Width(p.IDName)
	widths := make([]int, len(p.Columns))
	for i, c := range p.Columns {
		widths[i] = kit.Width(c.Name)
	}
	treeW := 0
	var measure func(n ProcNode, depth int)
	measure = func(n ProcNode, depth int) {
		idW = max(idW, kit.Width(n.ID))
		for i, c := range n.Cells {
			if i < len(widths) {
				widths[i] = max(widths[i], kit.Width(c.Text))
			}
		}
		treeW = max(treeW, 3*depth+kit.Width(n.Name))
		for _, c := range n.Children {
			measure(c, depth+1)
		}
	}
	measure(p.Root, 0)

	// Keep columns while the tree column has room for the names and a tag.
	cols := make([]int, len(p.Columns))
	for i := range cols {
		cols[i] = i
	}
	used := func() int {
		n := 1 + idW + 2
		for _, i := range cols {
			n += widths[i] + 2
		}
		return n
	}
	const tagRoom = 12
	for used()+max(treeW+tagRoom, kit.Width(p.TreeName)) > inner-1 {
		drop := -1
		for j, i := range cols {
			if p.Columns[i].Drop > 0 && (drop < 0 || p.Columns[i].Drop > p.Columns[cols[drop]].Drop) {
				drop = j
			}
		}
		if drop < 0 {
			break
		}
		cols = slices.Delete(cols, drop, drop+1)
	}
	room := max(1, inner-1-used())

	head := " " + k.S(kit.Muted, lpad(p.IDName, idW))
	for _, i := range cols {
		head += "  " + k.S(kit.Muted, pad(p.Columns[i].Name, widths[i], p.Columns[i].Right))
	}
	head += "  " + k.S(kit.Muted, p.TreeName)
	if p.Count != "" && kit.Width(head)+kit.Width(p.Count)+2 <= inner {
		head = spread(head, k.S(kit.Accent, p.Count)+" ", inner)
	}
	body := []string{head, k.S(kit.Faint, strings.Repeat("─", inner))}

	var walk func(n ProcNode, prefix string, last bool, depth int)
	walk = func(n ProcNode, prefix string, last bool, depth int) {
		line := " " + k.S(kit.Rule, lpad(n.ID, idW))
		for _, i := range cols {
			c := Cell{}
			if i < len(n.Cells) {
				c = n.Cells[i]
			}
			line += "  " + k.S(c.Role, pad(c.Text, widths[i], p.Columns[i].Right))
		}
		branch := ""
		if depth > 0 {
			branch = prefix + map[bool]string{true: "└─ ", false: "├─ "}[last]
		}
		free := room - kit.Width(branch)
		name := n.Name
		if kit.Width(name) > free {
			name = kit.Fit(name, max(1, free))
		}
		free -= kit.Width(name)
		var tags []string
		tagW := 0
		for _, t := range n.Tags {
			tags = append(tags, k.S(t.Role, t.Text))
			tagW += kit.Width(t.Text) + 1
		}
		if tagW+1 > free {
			tags, tagW = nil, 0
		}
		args := ""
		if n.Args != "" && kit.Width(n.Args)+1 <= free-tagW-1 {
			args = " " + k.S(kit.Muted, n.Args)
		}
		tree := k.S(kit.Rule, branch) + k.S(n.NameRole, name) + args
		if len(tags) > 0 {
			tree = spread(tree, strings.Join(tags, " "), room)
		}
		body = append(body, line+"  "+tree)
		next := prefix
		if depth > 0 {
			next += map[bool]string{true: "   ", false: "│  "}[last]
		}
		for i, c := range n.Children {
			walk(c, next, i == len(n.Children)-1, depth+1)
		}
	}
	walk(p.Root, "", true, 0)

	body = append(body, k.S(kit.Faint, strings.Repeat("─", inner)))
	var foot []string
	if p.Load != "" {
		foot = append(foot, k.S(kit.Muted, p.Load+" ")+k.S(p.LoadRole, p.LoadValue))
	}
	for _, l := range p.Legend {
		foot = append(foot, k.S(l.Role, "● ")+k.S(kit.Muted, l.Label))
	}
	line := ""
	for _, f := range foot {
		if line != "" && kit.Width(line)+2+kit.Width(f) > inner-2 {
			body = append(body, " "+line)
			line = ""
		}
		if line != "" {
			line += "  "
		}
		line += f
	}
	if p.FootRight != "" && kit.Width(line)+kit.Width(p.FootRight)+4 <= inner-2 {
		line = spread(line, k.S(kit.Rule, p.FootRight), inner-2)
	} else if p.FootRight != "" {
		body = append(body, " "+line)
		line = k.S(kit.Rule, p.FootRight)
	}
	body = append(body, " "+line)

	title := ""
	if p.Title != "" {
		title = k.S(kit.Muted, p.Title)
	}
	return kit.Clip(width, k.Draw(kit.Box{Title: title, Role: kit.Rule, Width: w}, body))
}
