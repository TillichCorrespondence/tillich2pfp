import glob
import os
from rdflib import Graph

prefix = "tillich"

files = glob.glob("./datasets/*.nt")
out_file = os.path.join("datasets", f"{prefix}.nt")
g = Graph()
for x in files:
    print(x)
    g.parse(x)
    os.unlink(x)
print(f"serializing graph to {out_file}")
g.serialize(out_file, format="nt", encoding="utf-8")
