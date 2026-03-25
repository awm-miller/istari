"""Temporary debug script to examine graph data."""
import re, json, sys

html = open('output/latest_graph.html', 'r', encoding='utf-8').read()

# Extract nodes
m = re.search(r'const allNodes = (\[.*?\]);\nconst allEdges', html, re.DOTALL)
nodes = json.loads(m.group(1))

# Extract edges - find start and end
edge_start = html.index('const allEdges = [') + len('const allEdges = ')
bracket_count = 0
edge_end = edge_start
for i, ch in enumerate(html[edge_start:], edge_start):
    if ch == '[': bracket_count += 1
    elif ch == ']': bracket_count -= 1
    if bracket_count == 0:
        edge_end = i + 1
        break
edges = json.loads(html[edge_start:edge_end])

nodeById = {n['id']: n for n in nodes}

cmd = sys.argv[1] if len(sys.argv) > 1 else 'summary'

if cmd == 'summary':
    print(f"Total nodes: {len(nodes)}")
    print(f"Total edges: {len(edges)}")
    from collections import Counter
    kinds = Counter(n.get('kind','?') for n in nodes)
    print(f"Node kinds: {dict(kinds)}")
    edge_kinds = Counter(e.get('kind','?') for e in edges)
    print(f"Edge kinds: {dict(edge_kinds)}")

elif cmd == 'search':
    query = sys.argv[2].lower()
    matches = [n for n in nodes if query in n.get('label','').lower()]
    print(f"Nodes matching '{query}': {len(matches)}")
    for n in matches:
        print(f"  {n['id']} | {n['label']} | kind={n.get('kind','')} lane={n.get('lane','')} registry_type={n.get('registry_type','')}")
        n_edges = [e for e in edges if e['source'] == n['id'] or e['target'] == n['id']]
        for e in n_edges:
            other_id = e['target'] if e['source'] == n['id'] else e['source']
            other = nodeById.get(other_id, {})
            print(f"    -> {other.get('label','?')} ({e['kind']}) [lane={other.get('lane','')}]")

elif cmd == 'seed_names':
    query = sys.argv[2].lower()
    matches = [n for n in nodes if query in n.get('label','').lower()]
    for n in matches:
        print(f"{n['label']}: seed_names={n.get('seed_names',[])}  appears_under={n.get('appears_under_identities','N/A')}")
