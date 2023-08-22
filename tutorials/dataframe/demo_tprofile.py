from dask.distributed import LocalCluster, Client
import ROOT

from DistRDF import LiveVisualize

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

def create_connection():
    cluster = LocalCluster(n_workers=20, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

if __name__ == "__main__":
    connection = create_connection()
    num_entries = 100000000
    df = RDataFrame(num_entries, daskclient=connection)

    mean_x, mean_y, mean_z = 0.0, 0.0, 0.0
    sigma_x, sigma_y, sigma_z = 4.0, 2.0, 1.0

    df = df.Define("x", f"gRandom->Gaus(10*rdfentry_/{num_entries}, {sigma_x})")\
           .Define("y", f"gRandom->Gaus(10*rdfentry_/{num_entries}, {sigma_y})")\
           .Define("z", f"sqrt(x * x + y * y)")
                           
    hprof1d = df.Profile1D(("hprof1d", "Profile of pz versus px", 100, -25, 35), "x", "y")
    hprof2d = df.Profile2D(("hprof2d", "Profile of pz versus px and py", 100, -20, 20, 100, -20, 20, -20, 20), "x", "y", "z")

    c1 = ROOT.TCanvas("c1", "Profile histogram example", 200, 10, 1000, 500)
    LiveVisualize([hprof1d])
    hprof1d.Draw()

    '''c2 = ROOT.TCanvas("c2", "Profile2D histogram example", 200, 10, 1000, 500)
    LiveVisualize([hprof2d])
    hprof2d.Draw("colz")'''