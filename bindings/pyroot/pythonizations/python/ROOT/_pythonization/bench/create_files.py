import ROOT
import random
from array import array

TREENAME = "Events"
N_FILES = 3
N_ENTRIES = 1_000

AUTO_FLUSHES = [1_000, 2_00, 500]
# Expected clusters: 1, 5, 2

for i in range(N_FILES):
    fname = f"test_file_{i+1}.root"
    flush = AUTO_FLUSHES[i]

    f = ROOT.TFile(fname, "RECREATE")
    tree = ROOT.TTree(TREENAME, TREENAME)
    tree.SetAutoFlush(flush)

    buf_a = array('f', [0.0])
    buf_b = array('i', [0])
    tree.Branch("A", buf_a, "A/F")
    tree.Branch("B", buf_b, "B/I")

    for _ in range(N_ENTRIES):
        buf_a[0] = random.gauss(0, 1)
        buf_b[0] = int(random.gauss(0, 1))
        tree.Fill()

    tree.Write()
    f.Close()
    print(f"Created {fname}  ({N_ENTRIES} entries, autoflush={flush}, ~{N_ENTRIES//flush} clusters)")

print(f"\nExpected total cluster ranges from GetClusterRanges: ~{sum(N_ENTRIES//f for f in AUTO_FLUSHES)}")
print(f"Expected offsets: file_0=[0,{N_ENTRIES}), file_1=[{N_ENTRIES},{2*N_ENTRIES}), file_2=[{2*N_ENTRIES},{3*N_ENTRIES})")