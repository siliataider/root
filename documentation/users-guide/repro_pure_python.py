import ROOT
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep

# =============== rdf repro ===============

# @ROOT.RDF.cpp_signature(ret=float, args=[ROOT.Math.PtEtaPhiMVector])
# def get_m(x):
#     return x.M()

# def get_m(x: ROOT.Math.PtEtaPhiMVector) -> float:
#     return x.M()

# for both cases above I get a parsing issue:
# Creating FunctionPointerConverter for fulltype 'double (*)(ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double> >)' resolved to 'double(*)(ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double> >'

def make_write_hist(entry: int) -> ROOT.TH1D:
    hname = f"h_{entry}"
    h = ROOT.TH1D(hname, hname, 10, 0, 10)
    h[...] = np.random.uniform(0, 10, 10)
    with ROOT.TFile.Open("bench_out/histos.root", "recreate") as f:
        f.WriteObject(h, hname)
    return h

def get_nbins_and_rebin(hist: ROOT.TH1D, rebin_factor: int) -> int:
    with ROOT.TFile.Open("bench_out/histos.root", "read") as f:
        stored = f.Get(hist.GetName())
        stored.Rebin(rebin_factor)
        return stored.GetNbinsX()
    
def read_remote_file(x: int) -> bool:
    url = "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root"
    with ROOT.TFile.Open(url, "read") as f:
        return f.IsOpen()

def save_hist_plot(hist: ROOT.TH1D) -> str:
    filename = f"bench_out/{hist.GetName()}.png"
    fig, ax = plt.subplots()
    hep.histplot(hist, ax=ax)
    fig.savefig(filename)
    plt.close(fig)
    return filename

def get_m(x: 'ROOT::Math::PtEtaPhiMVector') -> 'double':
    return x.M()

rdf = ROOT.RDataFrame(10) \
        .Define("x", "(int)rdfentry_") \
        .Define("p", "ROOT::Math::PtEtaPhiMVector(10, 0.1*rdfentry_, 0, 0.1)") \
        .Define("m", get_m, ["p"])
        # .Define("file_opened", read_remote_file, ["x"])
        # .Define("hist", make_write_hist, ["x"]) \
        # .Define("nbins_rebinned", get_nbins_and_rebin, ["hist", "x"]) \
        # .Define("hist_png", save_hist_plot, ["hist"])
        # .Define("vec_nbins", make_vector_of_nbins, ["x"])
        # .Define("vec_hists", make_vector_of_hists, ["nbins_rebinned"]) \
        # .Define("avg_int", compute_avg_integral, ["vec_hists"])

rdf.Display().Print()


# def make_vector_of_nbins(n: int) -> ROOT.std.vector[int]:
#     vec = ROOT.std.vector('int')()
#     for i in range(n):
#         vec.push_back(i+1)
#     return vec

# def make_vector_of_hists(n: int) -> ROOT.std.vector[ROOT.TH1D]:
#     ROOT.gInterpreter.GenerateDictionary(
#         "vector<TH1D>"
#     )
#     vec = ROOT.std.vector('TH1D')()
#     for i in range(n):
#         h = ROOT.TH1D(f"h_{i}", "h_{i}", 10, 0, 1)
#         h.FillRandom("gauss")
#         vec.push_back(h)
#     return vec

# def compute_avg_integral(hists: ROOT.RVec[ROOT.TH1D]) -> float:
#     total = sum(h.Integral() for h in hists)
#     return total / len(hists)

























# ROOT.gInterpreter.Declare("""
# extern "C" int MyFunction(int x) {
#     return x * 2;
# }
# """)

# addr = ROOT.gInterpreter.ProcessLine("(void*)&MyFunction")
# print(f"MyFunction address: {addr:#x}")

# rdf = ROOT.RDataFrame(5).Define["int(int)"](addr)
