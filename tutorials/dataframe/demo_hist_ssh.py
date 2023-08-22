from dask.distributed import Client, SSHCluster
import ROOT
import time, os

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

from DistRDF import LiveVisualize


def create_connection(ip_address, username):
    cluster = SSHCluster(
        [ip_address, ip_address],
        connect_options={
            "known_hosts": None,
            "username" : username
            },
        worker_options={"nthreads": 1,
                       "n_workers": 4,
                       },
    )
    client = Client(cluster)
    return client, cluster

def ssh_connection():
    client = Client("128.141.187.85:8786")
    return client


# Callback functions
def set_random_fill_color(hist):
    hist.SetFillColor(ROOT.kBlue)

def fit_gaus(hist):
    hist.Fit("gaus")

def fit_exp(hist):
    hist.Fit("expo")

def write_histogram_to_tfile(hist):
    filename = "/home/siliataider/Documents/root-project-folder/Output/output_file.root"
    file_exists = os.path.exists(filename)
    file = ROOT.TFile(filename, "UPDATE" if file_exists else "RECREATE")
    hist.Write()
    file.Close()

def modify_histogram_values(hist):
    for i in range(1, hist.GetNbinsX() + 1):
        hist.SetBinContent(i, hist.GetBinContent(i) * 2)

def delete_histogram(hist):
    hist.Delete()

def two_args(a, b):
    print(a, b)

def add_histogram(hist):
    added_hist = hist.Clone()
    hist.Add(added_hist)


if __name__ == "__main__":
    remote_ip = "128.141.187.85"
    remote_username = "rootuser"

    #connection, cluster = create_connection(remote_ip, remote_username)
    connection = ssh_connection()
    
    df = RDataFrame(1000000, daskclient=connection)

    

    ROOT.gRandom.SetSeed(1)
    df_1 = df.Define("gaus", "gRandom->Gaus(10, 1)").Define("exponential", "gRandom->Exp(10)")

    h_random = df.Define("random", "gRandom->Rndm() * 30").Histo1D(("random", "Random distribution", 50, 0, 30), "random")
    h_exp = df.Define("exponential1", "gRandom->Exp(10)").Histo1D(("exponential1", "Exponential distribution 1", 50, 0, 30), "exponential1")
    h_gaus = df_1.Histo1D(("gaus", "Normal distribution", 50, -20, 20), "gaus")
    h_exp2 = df.Define("exponential2", "gRandom->Exp(20)").Histo1D(("exponential2", "Exponential distribution 2", 50, 0, 100), "exponential2")

    h_sum = df_1.Sum("gaus")
    h_mean = df_1.Mean("gaus")
    h_max = df_1.Max("exponential") 

    

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
    
    #h_random.SetFillColor(ROOT.kBlue)

    LiveVisualize(histogram_callback_dict, set_random_fill_color)

        
    #h_exp.SetFillColor(ROOT.kRed)

    c = ROOT.TCanvas("distrdf002", "distrdf002", 1400, 700)
    c.Divide(2, 2)

    c.cd(1)
    h_gaus.Draw()
    c.cd(2)
    h_exp.Draw()
    c.cd(3)
    h_random.Draw()
    c.cd(4)
    h_exp2.Draw() 

    c.Update()

    print("sum: ", h_sum.GetValue())
    print("mean: ", h_mean.GetValue())

    time.sleep(2)
    print("Done!")
