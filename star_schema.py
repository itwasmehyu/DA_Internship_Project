import matplotlib.pyplot as plt
import networkx as nx

# Các node Dim và Fact
dims = ["dim_user", "dim_time", "dim_level", "dim_ad"]
facts = ["fact_engagement", "fact_revenue_iap", "fact_revenue_ads",
         "fact_gameplay", "fact_coin_gem", "fact_tutorial", "fact_uninstall"]

# Tạo graph
G = nx.Graph()

# Thêm node
for d in dims:
    G.add_node(d, type='dim')
for f in facts:
    G.add_node(f, type='fact')

# Kết nối Dim với Fact
edges = [
    ("dim_user", "fact_engagement"),
    ("dim_time", "fact_engagement"),

    ("dim_user", "fact_revenue_iap"),
    ("dim_time", "fact_revenue_iap"),

    ("dim_user", "fact_revenue_ads"),
    ("dim_time", "fact_revenue_ads"),
    ("dim_ad", "fact_revenue_ads"),

    ("dim_user", "fact_gameplay"),
    ("dim_time", "fact_gameplay"),
    ("dim_level", "fact_gameplay"),

    ("dim_user", "fact_coin_gem"),
    ("dim_time", "fact_coin_gem"),

    ("dim_user", "fact_tutorial"),
    ("dim_time", "fact_tutorial"),

    ("dim_user", "fact_uninstall")
]
G.add_edges_from(edges)

# Màu node
color_map = []
for node in G.nodes(data=True):
    if node[1]['type'] == 'dim':
        color_map.append('#4CAF50')  # xanh lá cho dimension
    else:
        color_map.append('#2196F3')  # xanh dương cho fact

# Vẽ
plt.figure(figsize=(10, 7))
pos = nx.spring_layout(G, seed=42, k=0.8)
nx.draw(G, pos, with_labels=True, node_color=color_map, node_size=2500,
        font_size=9, font_color='black', font_weight='bold', edge_color="#888")
plt.title("Star Schema Data Model", fontsize=14, fontweight='bold', color='black')
plt.show()
