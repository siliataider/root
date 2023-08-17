from dask.distributed import LocalCluster, Client
import ROOT
 
from DistRDF import live_visualize

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

def create_connection():
    cluster = LocalCluster(n_workers=20, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

def set_marker(graph):
    graph.SetMarkerStyle(5)

if __name__ == "__main__":

    connection = create_connection()
    num_entries = 200
    d = RDataFrame(num_entries, daskclient=connection)
    
    dd = d.Define("x", "rdfentry_").Define("y", "x*x")
    
    graph = dd.Graph("x", "y")

    live_visualize({graph: set_marker})
    
    c = ROOT.TCanvas()
    graph.Sort()
    graph.Draw("AP")