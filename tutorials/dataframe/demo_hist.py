from dask.distributed import LocalCluster, Client
import ROOT
import os

from DistRDF import LiveVisualize

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

def create_connection():
    cluster = LocalCluster(n_workers=5, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

# 1. A styling callback function
def set_random_fill_color(hist):
    hist.SetFillColor(ROOT.kBlue)

# 2. A fitting callback function
def fit_gaus(hist):
    hist.Fit("gaus")

def fit_exp(hist):
    hist.Fit("expo")

# 3. TODO A writing the hist to a tfile.WriteObject(h, "name") callback function 
def write_histogram_to_tfile(hist):
    filename = "/home/siliataider/Documents/root-project-folder/Output/output_file.root"
    file_exists = os.path.exists(filename)
    # ??? SHould we keep the file open to avoid closing and reopening many times?
    # Can we open the file while the program is trying to write on it?
    file = ROOT.TFile(filename, "UPDATE" if file_exists else "RECREATE")
    hist.Write()
    # file.WriteObject(hist, hist.GetName())
    # rootls filename
    file.Close()

# 4. -SHOULD FAIL- A callback function to change the values of the histogram: e.g., multiply each bin content by 2)
def modify_histogram_values(hist):
    for i in range(1, hist.GetNbinsX() + 1):
        hist.SetBinContent(i, hist.GetBinContent(i) * 2)

# 5. -SHOULD FAIL- A callback function to delete the histogram
def delete_histogram(hist):
    hist.Delete()

# 6. -SHOULD FAIL- A callback function with 2 args
def two_args(a, b):
    print(a, b)

# 7. -SHOULD FAIL- A callback function to change the values of the histogram by adding another histogram
def add_histogram(hist):
    added_hist = hist.Clone()
    hist.Add(added_hist)


if __name__ == "__main__":

    connection = create_connection()
    df = RDataFrame(1000000, daskclient=connection)

    c = ROOT.TCanvas("distrdf002", "distrdf002", 1400, 700)

    ROOT.gRandom.SetSeed(1)
    df_1 = df.Define("gaus", "gRandom->Gaus(10, 1)").Define("exponential1", "gRandom->Exp(10)").Define("random", "gRandom->Rndm() * 30").Define("exponential2", "gRandom->Exp(20)")

    h_random = df_1.Histo1D(("random", "Random distribution", 50, 0, 30), "random")
    h_exp = df_1.Histo1D(("exponential1", "Exponential distribution 1", 50, 0, 30), "exponential1")
    h_gaus = df_1.Histo1D(("gaus", "Normal distribution", 50, -20, 20), "gaus")
    h_exp2 = df_1.Histo1D(("exponential2", "Exponential distribution 2", 50, 0, 100), "exponential2")

    c.Divide(2, 2)

    """
    1. Pass a list of histograms
    """
    #LiveVisualize([h_random, h_exp, h_gaus])

    """ 
    2. Pass a list of histograms and a function to be executed on all of them
    """
    #LiveVisualize([h_exp2, h_exp, h_gaus, h_random], set_random_fill_color)

    histogram_callback_dict = {
        h_exp2: fit_exp,
        h_exp: delete_histogram,
        h_random: None,
        h_gaus: fit_gaus
    }  

    """ 
    3. Pass a dictionary of histograms and callback functions to be executed on each
    """
    #LiveVisualize(histogram_callback_dict)

    """ 
    4. Pass a dictionary of histograms and callback functions to be executed on each
       plus a global callback function to be executed on all histograms
    """
    
    #LiveVisualize(histogram_callback_dict, set_random_fill_color)

    c.cd(1)
    h_gaus.Draw()
    c.cd(2)
    h_exp.Draw()
    c.cd(3)
    h_random.Draw()
    c.cd(4)
    h_exp2.Draw()

    c.Update()

    print("Done!")