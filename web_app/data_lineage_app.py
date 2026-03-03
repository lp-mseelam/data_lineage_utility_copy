import os
import json
from typing import List, Optional, Tuple, Dict, Set
from collections import deque

import streamlit as st
import pandas as pd
import networkx as nx
from pyvis.network import Network
from streamlit.components.v1 import html as st_html
from streamlit_extras.metric_cards import style_metric_cards

st.set_page_config(page_title="Data Lineage Utility", layout="wide")
st.title("Data Lineage Utility")

@st.cache_data(show_spinner=False)
def load_excel(path_or_file, sheet_name="lineage") -> Optional[pd.DataFrame]:
    try:
        return pd.read_excel(path_or_file, sheet_name=sheet_name, engine="openpyxl")
    except Exception as e:
        st.error(f"Failed to load Excel sheet '{sheet_name}': {e}")
        return None

def load_from_snowflake(user, password, account, database, schema, table) -> pd.DataFrame:
    import snowflake.connector
    try:
        conn = snowflake.connector.connect(
            user=user, password=password, account=account,
            database=database, schema=schema,
        )
        sql = f"SELECT SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, RELATION FROM {database}.{schema}.{table}"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Snowflake load failed: {e}")
        return pd.DataFrame()

def norm(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def build_edges(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    required = ["source_schema", "source_table", "target_schema", "target_table"]
    for c in required:
        if c not in df.columns:
            st.error(f"Missing required column: {c}")
            return pd.DataFrame()
    for c in required:
        df[c] = df[c].astype(str).str.strip()
    if "relationship" in df.columns:
        df["relation"] = df["relationship"].astype(str).str.strip()
    elif "relation" in df.columns:
        df["relation"] = df["relation"].astype(str).str.strip()
    cols = required + (["relation"] if "relation" in df.columns else [])
    return df.dropna(subset=required)[cols]

def make_node(schema: str, table: str) -> str:
    return f"{schema}.{table}"

def get_node_schema(node: str) -> str:
    return node.rsplit(".", 1)[0] if "." in node else node

def build_graph(edges: pd.DataFrame) -> nx.DiGraph:
    G = nx.DiGraph()
    rel_present = "relation" in edges.columns
    for _, r in edges.iterrows():
        src = make_node(r["source_schema"], r["source_table"])
        tgt = make_node(r["target_schema"], r["target_table"])
        rel = r["relation"] if rel_present and pd.notna(r["relation"]) else ""
        G.add_edge(src, tgt, relation=rel)
    return G

def bfs_neighborhood(G: nx.DiGraph, center: str, direction: str, depth: int) -> Set[str]:
    if center not in G:
        return set()
    nodes = {center}
    frontier = {center}
    for _ in range(depth):
        if not frontier:
            break
        nxt = set()
        for n in frontier:
            if direction in ("both", "upstream"):
                nxt.update(u for u, _ in G.in_edges(n))
            if direction in ("both", "downstream"):
                nxt.update(v for _, v in G.out_edges(n))
        nodes |= nxt
        frontier = nxt
    return nodes

def focused_subgraph(G: nx.DiGraph, center_node: Optional[str], direction: str, depth: int,
                     allowed_schemas: Optional[List[str]]) -> nx.DiGraph:
    allowed_norms = {norm(s) for s in allowed_schemas} if allowed_schemas else None

    if not center_node or center_node == "-- All Tables --":
        if not allowed_norms:
            return G.copy()
        kept = {n for n in G.nodes() if norm(get_node_schema(n)) in allowed_norms}
        return G.subgraph(kept).copy()

    hood = bfs_neighborhood(G, center_node, direction, depth)
    if allowed_norms:
        hood = {n for n in hood if (norm(get_node_schema(n)) in allowed_norms or n == center_node)}
    return G.subgraph(hood).copy()

def compute_levels_focused(H: nx.DiGraph, center_node: str) -> dict:
    levels = {center_node: 0}
    q = deque([center_node])
    while q:
        u = q.popleft()
        for v in H.successors(u):
            lv = levels[u] + 1
            if v not in levels or lv > levels[v]:
                levels[v] = lv
                q.append(v)
    q = deque([center_node])
    while q:
        u = q.popleft()
        for v in H.predecessors(u):
            lv = levels[u] - 1
            if v not in levels or lv < levels[v]:
                levels[v] = lv
                q.append(v)
    shift = -min(levels.values())
    return {k: v + shift for k, v in levels.items()}

def compute_levels_global(H: nx.DiGraph) -> dict:
    if H.number_of_nodes() == 0:
        return {}
    roots = [n for n in H.nodes() if H.in_degree(n) == 0]
    if not roots:
        roots = [next(iter(H.nodes()))]
    levels = {}
    q = deque([(r, 0) for r in roots])
    for r in roots:
        levels[r] = 0
    while q:
        u, lu = q.popleft()
        for v in H.successors(u):
            lv = max(lu + 1, levels.get(v, float("-inf")))
            if v not in levels or lv > levels[v]:
                levels[v] = lv
                q.append((v, lv))
    for n in H.nodes():
        levels.setdefault(n, 0)
    return levels

def render_pyvis(H: nx.DiGraph, center_node: Optional[str], node_shape: str, layout_mode: str):
    if H.number_of_nodes() == 0:
        st.info("No nodes to render for the current filters/selection.")
        return

    net = Network(height="750px", width="100%", directed=True, bgcolor="#ffffff", font_color="#202124")

    hierarchical = layout_mode in ("Hierarchical (Left → Right)", "Hierarchical (Top → Bottom)")
    if hierarchical:
        vis_direction = "LR" if layout_mode == "Hierarchical (Left → Right)" else "UD"
        opts = {
            "layout": {
                "hierarchical": {
                    "enabled": True,
                    "direction": vis_direction,
                    "sortMethod": "directed",
                    "levelSeparation": 220,
                    "nodeSpacing": 180,
                    "treeSpacing": 260
                }
            },
            "physics": {"enabled": False},
            "interaction": {"hover": True, "tooltipDelay": 150}
        }
        net.set_options(json.dumps(opts))
    else:
        fd_opts = {
            "physics": {
                "enabled": True,
                "solver": "forceAtlas2Based",
                "forceAtlas2Based": {"gravitationalConstant": -90, "springLength": 140, "springConstant": 0.09},
                "minVelocity": 0.75
            },
            "interaction": {"hover": True, "tooltipDelay": 150}
        }
        net.set_options(json.dumps(fd_opts))

    if hierarchical:
        if center_node and center_node in H:
            level_map = compute_levels_focused(H, center_node)
        else:
            level_map = compute_levels_global(H)
    else:
        level_map = {}

    for n in H.nodes():
        is_center = (center_node == n)
        label = n.split(".")[-1]
        title = n
        color = "#ffa726" if is_center else "#1a73e8"
        kwargs = {"label": label, "title": title, "color": color, "shape": node_shape, "borderWidth": 2}
        if hierarchical and n in level_map:
            kwargs["level"] = int(level_map[n])
        net.add_node(n, **kwargs)

    for u, v in H.edges():
        rel = H.get_edge_data(u, v, {}).get("relation", "")
        net.add_edge(u, v, title=rel, color="#9aa0a6", arrows="to")

    st_html(net.generate_html(), height=750, scrolling=True)

def graph_kpis(G: nx.DiGraph) -> Dict[str, int]:
    roots = [n for n in G.nodes() if G.in_degree(n) == 0]
    sinks = [n for n in G.nodes() if G.out_degree(n) == 0]
    comps = nx.number_weakly_connected_components(G) if G.number_of_nodes() else 0
    return {
        "total_tables": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "roots": len(roots),
        "sinks": len(sinks),
        "components": comps,
    }

def focused_kpis(G: nx.DiGraph, center: Optional[str], direction: str, depth: int) -> Dict[str, int]:
    if not center or center not in G:
        return {"upstream_nodes": 0, "downstream_nodes": 0, "total_in_scope": 0}
    up = bfs_neighborhood(G, center, "upstream" if direction != "downstream" else "upstream", depth)
    down = bfs_neighborhood(G, center, "downstream" if direction != "upstream" else "downstream", depth)
    scope = bfs_neighborhood(G, center, direction, depth)
    return {
        "upstream_nodes": max(len(up) - 1, 0) if up else 0,
        "downstream_nodes": max(len(down) - 1, 0) if down else 0,
        "total_in_scope": len(scope),
    }

def diagnose_visibility(G: nx.DiGraph, H: nx.DiGraph, center_node: Optional[str],
                        direction: str, depth: int,
                        allowed_schemas: Optional[List[str]], all_schemas: List[str]) -> None:
    msgs = []
    if center_node and center_node not in G:
        candidates = [n for n in G.nodes() if norm(n) == norm(center_node) or norm(n.split(".")[-1]) == norm(center_node.split(".")[-1])]
        hint = f" Did you mean: {', '.join(candidates[:5])}?" if candidates else ""
        msgs.append(f"Focused table not found in graph: {center_node}.{hint}")
    allowed_norms = {norm(s) for s in allowed_schemas} if allowed_schemas else set()
    if allowed_schemas:
        pass_ct = sum(1 for n in G.nodes() if norm(get_node_schema(n)) in allowed_norms)
        if pass_ct == 0:
            msgs.append("Schema filter removed all nodes. Clear the filter or add more schemas.")
        elif center_node and norm(get_node_schema(center_node)) not in allowed_norms:
            msgs.append("Focused table’s schema is not in the filter. Keeping it visible, but neighbors may be hidden.")
    if center_node and center_node in G:
        full_scope = bfs_neighborhood(G, center_node, "both", max(1, depth))
        if len(H.nodes()) <= 1 and len(full_scope) > 1 and direction != "both":
            msgs.append("No neighbors in chosen direction within current depth. Try direction='both' or increase hops.")
    if H.number_of_nodes() == 0:
        msgs.append("No nodes to render after applying selection and filters.")
    if msgs:
        with st.expander("Diagnostics"):
            for m in msgs:
                st.info(m)

st.sidebar.header("Input Source")
source_mode = st.sidebar.radio("Load lineage from:", ["📂 Local Excel (default)", "📄 Upload Excel", "🗄️ Database"])

raw_df = pd.DataFrame()
if source_mode == "📂 Local Excel (default)":
    paths = ["DataLineage.xlsx", os.path.join("data", "DataLineage.xlsx")]
    path = next((p for p in paths if os.path.exists(p)), None)
    if path:
        raw_df = load_excel(path)
elif source_mode == "📄 Upload Excel":
    upload = st.sidebar.file_uploader("Upload Lineage", type=["xlsx"])
    if upload:
        raw_df = load_excel(upload)
else:
    with st.sidebar.expander("DB Settings"):
        user = st.text_input("User")
        pwd = st.text_input("Password", type="password")
        acct = st.text_input("Account")
        db = st.text_input("Database")
        sch = st.text_input("Schema")
        tbl = st.text_input("Table")
        if st.button("Load"):
            raw_df = load_from_snowflake(user, pwd, acct, db, sch, tbl)

if raw_df is None or raw_df.empty:
    st.info("Please provide a data source to begin.")
    st.stop()

edges = build_edges(raw_df)
if edges.empty:
    st.warning("No edges available in the provided data.")
    st.stop()

G = build_graph(edges)
all_nodes = sorted(G.nodes())
schemas = sorted(set(edges["source_schema"]).union(set(edges["target_schema"])))

st.sidebar.markdown("---")
st.sidebar.header("Visual Settings")
node_shape = st.sidebar.radio("Node Shape", ["box", "dot"], index=0)
direction = st.sidebar.radio("Lineage Direction", ["both", "upstream", "downstream"])
depth = st.sidebar.slider("Depth (hops)", 1, 5, 2)
layout_mode = st.sidebar.selectbox("Layout", ["Hierarchical (Left → Right)", "Hierarchical (Top → Bottom)", "Force Directed"])
allowed_schemas = st.sidebar.multiselect("Filter by schema", schemas, default=schemas)
if len(allowed_schemas) == 0:
    allowed_schemas = None

k = graph_kpis(G)
m1, m2, m3, m4, m5 = st.columns(5)

st.markdown(
    """
    <style>
    [data-testid="stMetricLabel"] p {
        font-weight: 800 !important;
        font-size: 1rem !important;
        color: #1f2937 !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

m1.metric("Total Tables", k["total_tables"])
m2.metric("Relationships", k["total_edges"])
m3.metric("Unique Schemas", len(schemas))
m4.metric("Root Tables", k["roots"], help="No upstream dependencies")
m5.metric("Sink Tables", k["sinks"], help="No downstream dependencies")

style_metric_cards(
    background_color="#F8F9FB",
    border_size_px=1,
    border_color="#CCC",
    border_radius_px=10,
    border_left_color="#CCC",
    box_shadow=True
)

st.markdown("---")
st.subheader("🎛 Selection & Filters")

search = st.text_input("Search tables", key="search")
filtered_nodes = [n for n in all_nodes if search.lower() in n.lower()] if search else all_nodes
selected_node = st.selectbox("Select Table Focus", ["-- All Tables --"] + filtered_nodes)
node_to_query = None if selected_node == "-- All Tables --" else selected_node

if node_to_query:
    fk = focused_kpis(G, node_to_query, direction, depth)
    c1, c2, c3 = st.columns(3)
    c1.metric("Upstream Nodes (within hops)", fk["upstream_nodes"])
    c2.metric("Downstream Nodes (within hops)", fk["downstream_nodes"])
    c3.metric("Nodes in Scope", fk["total_in_scope"])

st.markdown("---")
if node_to_query:
    st.subheader(f"⬆️⬇️ Lineage Paths: {node_to_query}")
    up = sorted(u for u, _ in G.in_edges(node_to_query))
    down = sorted(v for _, v in G.out_edges(node_to_query))
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Immediate Upstream**")
        if up:
            for u in up:
                st.code(u)
        else:
            st.caption("None")
    with c2:
        st.write("**Immediate Downstream**")
        if down:
            for d in down:
                st.code(d)
        else:
            st.caption("None")
else:
    st.subheader("📋 Complete Lineage List")
    disp_cols = ["SOURCE", "TARGET"]
    if "relation" in edges.columns:
        disp_cols.append("relation")
    st.dataframe(
        edges.assign(
            SOURCE=lambda x: x.source_schema + "." + x.source_table,
            TARGET=lambda x: x.target_schema + "." + x.target_table
        )[disp_cols],
        use_container_width=True
    )

st.markdown("---")
st.subheader("🕸 Graphical Lineage View")
H = focused_subgraph(G, node_to_query, direction, depth, allowed_schemas)
with st.container():
    st.caption(f"Rendering {H.number_of_nodes()} nodes and {H.number_of_edges()} edges")
diagnose_visibility(G, H, node_to_query, direction, depth, allowed_schemas, schemas)
render_pyvis(H, node_to_query, node_shape, layout_mode)