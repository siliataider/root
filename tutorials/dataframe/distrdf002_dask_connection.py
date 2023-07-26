## \file
## \ingroup tutorial_dataframe
## \notebook -draw
## Configure a Dask connection and fill two histograms distributedly.
##
## This tutorial shows the ingredients needed to setup the connection to a Dask
## cluster (e.g. a `LocalCluster` for a single machine). After this initial
## setup, an RDataFrame with distributed capabilities is created and connected
## to a Dask `Client` instance. Finally, a couple of histograms are drawn from
## the created columns in the dataset. Relevant documentation can be found at
## http://distributed.dask.org/en/stable .
##
## \macro_code
## \macro_image
##
## \date February 2022
## \author Vincenzo Eduardo Padulano
from dask.distributed import LocalCluster, Client
import ROOT
import time
import random
import os

# Import live_visualize function from backend.py
#from DistRDF.Backends.Dask.Backend import live_visualize
from DistRDF import live_visualize

# Point RDataFrame calls to Dask RDataFrame object
RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame


def create_connection():
    """
    Setup connection to a Dask cluster. Two ingredients are needed:
    1. Creating a cluster object that represents computing resources. This can be
       done in various ways depending on the type of resources at disposal. To use
       only the local machine (e.g. your laptop), a `LocalCluster` object can be
       used. This step can be skipped if you have access to an existing Dask
       cluster; in that case, the cluster administrator should provide you with a
       URL to connect to the cluster in step 2. More options for cluster creation
       can be found in the Dask docs at
       http://distributed.dask.org/en/stable/api.html#cluster .
    2. Creating a Dask client object that connects to the cluster. This accepts
       directly the object previously created. In case the cluster was setup
       externally, you need to provide an endpoint URL to the client, e.g.
       'https://myscheduler.domain:8786'.

    Through Dask, you can connect to various types of cluster resources. For
    example, you can connect together a set of machines through SSH and use them
    to run your computations. This is done through the `SSHCluster` class. For
    example:

    ```python
    from dask.distributed import SSHCluster
    cluster = SSHCluster(
        # A list with machine host names, the first name will be used as
        # scheduler, following names will become workers.
        hosts=["machine1","machine2","machine3"],
        # A dictionary of options for each worker node, here we set the number
        # of cores to be used on each node.
        worker_options={"nprocs":4,},
    )
    ```

    Another common usecase is interfacing Dask to a batch system like HTCondor or
    Slurm. A separate package called dask-jobqueue (https://jobqueue.dask.org)
    extends the available Dask cluster classes to enable running Dask computations
    as batch jobs. In this case, the cluster object usually receives the parameters
    that would be written in the job description file. For example:

    ```python
    from dask_jobqueue import HTCondorCluster
    cluster = HTCondorCluster(
        cores=1,
        memory='2000MB',
        disk='1000MB',
    )
    # Use the scale method to send as many jobs as needed
    cluster.scale(4)
    ```

    In this tutorial, a cluster object is created for the local machine, using
    multiprocessing (processes=True) on 2 workers (n_workers=2) each using only
    1 core (threads_per_worker=1) and 2GiB of RAM (memory_limit="2GiB").
    """
    cluster = LocalCluster(n_workers=5, threads_per_worker=1, processes=True, memory_limit="2GiB")
    client = Client(cluster)
    return client

# 1. A styling callback function
def set_random_fill_color(hist):
    hist.SetFillColor(random.randint(1, 9))

# 2. A fitting callback function
def fit(hist):
    # If I move to the right pad in the backend after calling the fit it breaks
    hist.Fit("gaus")

# 3. TODO A writing the hist to a tfile.WriteObject(h, "name") callback function 
def write_histogram_to_tfile(hist):
    # Check if the file exists
    filename = "/home/siliataider/Documents/root-project-folder/Output/output_file.root"
    file_exists = os.path.exists(filename)

    # Open the ROOT file in "UPDATE" mode if it exists, otherwise create a new file
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



# This tutorial uses Python multiprocessing, so the creation of the cluster
# needs to be wrapped in the main clause as described in the Python docs
# https://docs.python.org/3/library/multiprocessing.html
if __name__ == "__main__":

    # Create the connection to the mock Dask cluster on the local machine
    connection = create_connection()
    # Create an RDataFrame that will use Dask as a backend for computations
    df = RDataFrame(1000000, daskclient=connection)

    # Plot the histograms side by side on a canvas
    c = ROOT.TCanvas("distrdf002", "distrdf002", 1000, 600)

    # Set the random seed and define two columns of the dataset with random numbers.
    ROOT.gRandom.SetSeed(1)
    df_1 = df.Define("gaus", "gRandom->Gaus(10, 1)").Define("exponential", "gRandom->Exp(10)")
    df_2 = df.Define("random", "gRandom->Rndm() * 30")


    h_gaus = df_1.Histo1D(("gaus", "Normal distribution", 50, -20, 20), "gaus")
    h_sum = df_1.Sum("gaus")
    h_exp = df_1.Histo1D(("exponential", "Exponential distribution", 50, 0, 30), "exponential")
    h_random = df_2.Histo1D(("random", "Random distribution", 50, 0, 30), "random")
    h_mean = df_2.Mean("random")
    h_exp2 = df_1.Histo1D(("exponential2", "Exponential distribution 2", 20, 0, 70), "exponential")
    h_max = df_1.Max("exponential")  

    var = 5
    
    c.Divide(2, 2)

    #live_visualize([h_random, h_exp, h_gaus])
    #live_visualize([h_exp2, h_exp, h_gaus, h_random], two_args)

    histogram_callback_dict = {
        h_exp2: set_random_fill_color,
        h_exp: two_args,
        h_random: None,
        h_gaus: fit
    }  

    live_visualize(histogram_callback_dict)


    c.cd(1)
    h_gaus.Draw()
    c.cd(2)
    h_exp.Draw()
    c.cd(3)
    h_random.Draw()
    c.cd(4)
    h_exp2.Draw()

    print("h_sum ", h_sum.GetValue()) 
    print("h_mean ", h_mean.GetValue()) 
    print("h_max ", h_max.GetValue()) 
    

    c.Update()

    time.sleep(5)
    print("Done!")

'''
1. Raise an error when args are not histograms
2. Added a disctionary ro be handed to live_visualize instead of a list
'''