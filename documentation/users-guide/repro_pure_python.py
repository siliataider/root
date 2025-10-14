import ROOT 
import mplhep as hep 
import matplotlib.pyplot as plt 
import cppyy

# =============== tf1 repro ===============

# h = ROOT.TH1D("h", "h", 100, -5, 5) 
# # h.FillRandom("gaus", 1000) 

# def py_gaus(x, par): 
#     return 1 

# f_py = ROOT.TF1("f_py", py_gaus, -5, 5, 1) 
# h.Fit(f_py, "S") 
# print(h) 

# =============== cppyy repro ===============
# cppyy.cppdef("""
#     double call_with_value(double (*f)(double)) {
#         return f(3.0);
#     }
# """)

# def pyfun(x):
#     return x * 2

# print(cppyy.gbl.call_with_value(pyfun))

# =============== rdf repro ===============

# @ROOT.RDF.cpp_signature(ret=float, args=[ROOT.Math.PtEtaPhiMVector])
# def get_m(x):
#     return x.M()

# def get_m(x: ROOT.Math.PtEtaPhiMVector) -> float:
#     return x.M()

"""
for both cases above I get a parsing issue:
CreateConverter: creating FunctionPointerConverter for fulltype 'double (*)(ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double> >)' resolved to 'double(*)(ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double> >'
"""

# @ROOT.RDF.cpp_signature(ret=int, args=[ROOT.TH1D])
def get_nbins(hist: ROOT.TH1D, x: int) -> int:
    with ROOT.TFile.Open("root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root") as f1:
        f1.ls()

    with ROOT.TFile.Open("myfile.root","recreate") as f:
        h = ROOT.TH1D("h", "h", 10, 0, 10)
        f.WriteObject(h, "myobject")
    hist.Rebin(x)
    return hist.GetNbinsX()

rdf = ROOT.RDataFrame(10) \
        .Define("x", "(int)rdfentry_") \
        .Define("h", 'TH1D(Form("h_%d", x), "h", 10, 0, 10)') \
        .Define("nbins", get_nbins, ["h", "x"])

rdf.Display().Print()
