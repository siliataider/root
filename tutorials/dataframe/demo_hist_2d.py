from dask.distributed import LocalCluster, Client
import ROOT
import time
import subprocess

from DistRDF import live_visualize

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

def start_cpu_monitoring():
    cpu_monitoring_script = "/home/siliataider/Documents/silia-dev-root-project-folder/CPU_monitoring/cpu_monitor.py" 
    subprocess.Popen(["python", cpu_monitoring_script])

def create_connection():
    cluster = LocalCluster(n_workers=20, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

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

    live_visualize([normal_3d])

    normal_3d.Draw("COLZ CONT")
    c.Update()

    end_time = time.time()  
    elapsed_time = end_time - start_time
    
    print(f"Elapsed time: {elapsed_time} seconds")




'''
Elapsed time in seconds (10 workers, local cluster):

        | Without live_visualize()  |  With live_visualize() 
--------------------------------------------------------------
Histo1D |  9.106358051300049        |  8.95311713218689
--------------------------------------------------------------
Histo2D |  9.291945695877075        |  8.989943504333496
--------------------------------------------------------------
Histo3d |  12.687630414962769       |  33.36175608634949
--------------------------------------------------------------


#Define("x", "gRandom->Gaus(1.*rdfentry_/10000., 2*{sigma_x}")
df_ = df.Define("x", f"rdfentry_ < {num_entries/6} ? gRandom->Gaus(0, 2*{sigma_x}) : \
                        rdfentry_ < {2*num_entries/6} ? gRandom->Gaus(2, 2*{sigma_x}) : \
                        rdfentry_ < {3*num_entries/6} ? gRandom->Gaus(0, 2*{sigma_x}) : \
                        rdfentry_ < {4*num_entries/6} ? gRandom->Gaus(-1, 2*{sigma_x}) : \
                        rdfentry_ < {5*num_entries/6} ? gRandom->Gaus(2, 2*{sigma_x}) : \
                        gRandom->Gaus(-1, {sigma_y})") \
        .Define("y", f"rdfentry_ < {num_entries/6} ? gRandom->Gaus(1, 2*{sigma_y}) : \
                        rdfentry_ < {2*num_entries/6} ? gRandom->Gaus(2, 2*{sigma_y}) : \
                        rdfentry_ < {3*num_entries/6} ? gRandom->Gaus(1, 2*{sigma_y}) : \
                        rdfentry_ < {4*num_entries/6} ? gRandom->Gaus(0, 2*{sigma_y}) : \
                        rdfentry_ < {5*num_entries/6} ? gRandom->Gaus(2, 2*{sigma_y}) : \
                        gRandom->Gaus(0, {sigma_y})") \
        .Define("z", f"rdfentry_ < {num_entries/6} ? gRandom->Gaus({mean_z}, 0.3*{sigma_z}) : \
                        rdfentry_ < {2*num_entries/6} ? gRandom->Gaus({mean_z}, 0.6*{sigma_z}) : \
                        rdfentry_ < {3*num_entries/6} ? gRandom->Gaus({mean_z}, 0.2*{sigma_z}) : \
                        rdfentry_ < {4*num_entries/6} ? gRandom->Gaus({mean_z}, -0.2*{sigma_z}) : \
                        rdfentry_ < {5*num_entries/6} ? gRandom->Gaus({mean_z}, 0.3*{sigma_z}) : \
                        gRandom->Gaus({mean_z}, {sigma_z})") \
        .Define("x3d", f"gRandom->Gaus({mean_x}, 0.2*{sigma_x})") \
        .Define("y3d", f"gRandom->Gaus({mean_y}, 0.2*{sigma_y})")

'''
