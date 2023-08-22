from dask.distributed import LocalCluster, Client
import ROOT
import time
import subprocess

from DistRDF import LiveVisualize

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

def start_cpu_monitoring():
    cpu_monitoring_script = "/home/siliataider/Documents/silia-dev-root-project-folder/CPU_monitoring/cpu_monitor.py" 
    subprocess.Popen(["python", cpu_monitoring_script])

def create_connection():
    cluster = LocalCluster(n_workers=20, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

def set_fill_color(hist):
    hist.SetFillColor(ROOT.kBlue)

def fit_gaus(hist):
    hist.Fit("gaus")

if __name__ == "__main__":

    #start_cpu_monitoring()
    start_time = time.time()

    connection = create_connection()
    num_entries = 1000000
    df = RDataFrame(num_entries, daskclient=connection)

    sigma_x, sigma_y, sigma_z = 2.0, 3.0, 4.0

    c = ROOT.TCanvas("distrdf002", "distrdf002", 1000, 500)
    ROOT.gStyle.SetPalette(ROOT.kRainBow)          

    df_ = df.Define("x", f"gRandom->Gaus(10*rdfentry_/{num_entries}, {sigma_x})")\
            .Define("y", f"gRandom->Gaus(10*rdfentry_/{num_entries}, {sigma_y})")\
            .Define("z", f"gRandom->Gaus(10*rdfentry_/{num_entries}, {sigma_y})")

    normal_1d = df_.Histo1D(("normal_1d", "1D Histogram of a Normal Distribution", 
                    100, -10, 20
                    ), "x") 
    
    normal_2d = df_.Histo2D(("normal_2d", "2D Histogram of a Normal Distribution",
                    100, -20, 20, 
                    100, -20, 20
                    ), "x", "y")   

    normal_3d = df_.Histo3D(("normal_3d", "3D Histogram of a Normal Distribution",
                    50, -20, 20,  
                    50, -20, 20,  
                    50, -20, 20  
                    ), "x", "y", "z") 

    #LiveVisualize({normal_1d: set_fill_color}, fit_gaus)
    LiveVisualize([normal_1d, normal_2d])

    normal_1d.Draw("COLZ CONT")
    c.Update()

    end_time = time.time()  
    elapsed_time = end_time - start_time
    
    print(f"Elapsed time: {elapsed_time} seconds")