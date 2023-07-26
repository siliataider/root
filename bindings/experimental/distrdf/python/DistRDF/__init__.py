#  @author Vincenzo Eduardo Padulano
#  @author Enric Tejedor
#  @date 2021-02

################################################################################
# Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################
from __future__ import annotations

import logging
import sys
import types

import concurrent.futures

from typing import Iterable, TYPE_CHECKING

from DistRDF.Backends import build_backends_submodules

if TYPE_CHECKING:
    from DistRDF.Proxy import ResultPtrProxy, ResultMapProxy

logger = logging.getLogger(__name__)

from typing import List, Dict, Optional, Callable
import ast
import inspect
import warnings


def is_action_blocked(node):
    """
    Checks if the given Abstract Syntax Tree (AST) node corresponds to a blocked action.

    Args:
        node (ast.AST): The AST node to check.

    Returns:
        bool: True if the AST node corresponds to a blocked action,
              False otherwise.
    """

    BLOCKED_ACTIONS = ["Delete", "Add", "AddBinContent", "Build", "Divide", "DoFillN", "Fill", "FillN", 
                       "FillRandom", "Merge",  "Multiply", "Rebin", "Reset", "Scale", "SetBinContent", 
                       "SetBinError", "SetBins", "SetBinsLength", "SetCellContent", "SetDirectory", 
                       "SetEntries", "TransformHisto", "UpdateBinContent"]

    # Checking if this node is a function
    if isinstance(node, ast.Call):
        # Checking if we're calling an attribute of an object
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
            if func_name in BLOCKED_ACTIONS:
                return True

    return False


def is_callback_safe(callback):
    """
    Checks if the provided callback function is safe for live visualization, 
    meaning it does not contain blocked actions.

    Args:
        callback (function): The callback function to check.

    Returns:
        bool: True if the callback function is safe (does not contain blocked actions), False otherwise.
    """

    # Get the source code of the callback function
    callback_source = inspect.getsource(callback)

    # Parse the callback function's source code
    callback_source_ast = ast.parse(callback_source)

    for node in ast.walk(callback_source_ast):
        if is_action_blocked(node):
            return False
    return True


def is_valid_histogram(obj):
    """
    Checks if the object is a valid TH1 histogram.

    Args:
        obj: The object to be checked.

    Returns:
        bool: True if the object is a valid TH1 histogram, False otherwise.
    """
    import ROOT

    try:
        if obj.proxied_node.operation.name == "Histo1D":
            return True
        #return isinstance(obj.GetValue(), ROOT.TH1)
    except:
        return False


'''def live_visualize(histograms: List, callback=None) -> None:
    """
    Enables live visualization for the given histograms by setting the
    live_visualization_enabled flag of the Headnode to True.

    Args:
        histograms (List[ROOT.TH1D]): The list of histograms to enable live visualization for.
    """
    import ROOT
    from DistRDF import HeadNode

    valid_arg = True

    # TODO figure out how to check the type of the histograms without triggering the computation graph with .GetValue()
    
    for hist in histograms:
        valid_arg = is_valid_histogram(hist)
        print(valid_arg)
    
    if valid_arg:
        headnode = histograms[0].proxied_node.get_head() # Assuming all passed histograms share the same headnode
        headnode.live_visualization_enabled = True
        headnode.histogram_ids = [histogram.proxied_node.node_id for histogram in histograms]

        if callback:
            if callable(callback):
                if len(inspect.signature(callback).parameters) == 1:
                    if is_callback_safe(callback):
                        headnode.live_visualization_callback = callback
                    else:
                        warnings.warn("The provided callback function contains blocked actions. Skipping callback.")
                else:
                    warnings.warn("The callback function should have exactly one parameter. Skipping callback.")
            else:
                warnings.warn("The provided callback is not callable. Skipping callback.")
    else:
        raise ValueError("All elements in the 'histograms' list must be valid ROOT.TH1D histograms. Skipping live visualization.")'''
    

def live_visualize(histogram_callback_dict: Dict[type, Optional[Callable]]):
    """
    Enables live visualization for the given histograms by setting the
    live_visualization_enabled flag of the Headnode to True.

    Args:
        histogram_callback_dict (Dict[type, Optional[Callable]]): A dictionary where the keys are
            the histograms and the values are the corresponding callback functions. The callback
            functions are optional (can be set to None) if no callback is required for a specific histogram.
    """
    # Import the necessary ROOT classes inside the function to avoid circular dependency
    import ROOT
    from DistRDF import HeadNode


    histogram_id_callback_dict = {}

    for hist, callback in histogram_callback_dict.items():
        if not is_valid_histogram(hist):
            raise ValueError("All elements in the 'histograms' list must be valid ROOT.TH1D histograms. Skipping live visualization.")
        
        if callback:
            if callable(callback):
                if len(inspect.signature(callback).parameters) == 1:
                    if not is_callback_safe(callback):
                        callback = None
                        warnings.warn("The provided callback function contains blocked actions. Skipping callback: ")
                else:
                    callback = None
                    warnings.warn("The callback function should have exactly one parameter. Skipping callback.")
            else:
                callback = None
                warnings.warn("The provided callback is not callable. Skipping callback.")
        
        histogram_id_callback_dict[hist.proxied_node.node_id] =  callback


    headnode = list(histogram_callback_dict)[0].proxied_node.get_head()
    headnode.live_visualization_enabled = True
    headnode.histogram_id_callback_dict = histogram_id_callback_dict

            
'''
if len(callback.__code__.co_varnames) == 1:
if len(inspect.signature(callback).parameters) != 1:
'''


def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation. This method also executes the function on the current
    user environment so changes are visible on the running session.

    This allows users to inject and execute custom code on the worker
    environment without being part of the RDataFrame computational graph.

    Args:
        fun (function): Function to be executed.

        *args (list): Variable length argument list used to execute the
            function.

        **kwargs (dict): Keyword arguments used to execute the function.
    """
    from DistRDF.Backends import Base
    Base.BaseBackend.register_initialization(fun, *args, **kwargs)


def RunGraphs(proxies: Iterable) -> int:
    """
    Trigger the execution of multiple RDataFrame computation graphs on a certain
    distributed backend. If the backend doesn't support multiple job
    submissions concurrently, the distributed computation graphs will be
    executed sequentially.

    Args:
        proxies(list): List of action proxies that should be triggered. Only
            actions belonging to different RDataFrame graphs will be
            triggered to avoid useless calls.

    Return:
        (int): The number of unique computation graphs executed by this call.


    Example:

        @code{.py}
        import ROOT
        RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame
        RunGraphs = ROOT.RDF.Experimental.Distributed.RunGraphs

        # Create 3 different dataframes and book an histogram on each one
        histoproxies = [
            RDataFrame(100)
                .Define("x", "rdfentry_")
                .Histo1D(("name", "title", 10, 0, 100), "x")
            for _ in range(4)
        ]

        # Execute the 3 computation graphs
        n_graphs_run = RunGraphs(histoproxies)
        # Retrieve all the histograms in one go
        histos = [histoproxy.GetValue() for histoproxy in histoproxies]
        @endcode


    """
    # Import here to avoid circular dependencies in main module
    from DistRDF.Proxy import execute_graph
    import ROOT

    if not proxies:
        logger.warning("RunGraphs: Got an empty list of handles, now quitting.")
        return 0

    # Get proxies belonging to distinct computation graphs
    uniqueproxies = list({proxy.proxied_node.get_head(): proxy for proxy in proxies}.values())

    if ROOT.IsImplicitMTEnabled():
        # Submit all computation graphs concurrently from multiple Python threads.
        # The submission is not computationally intensive
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(uniqueproxies)) as executor:
            futures = [executor.submit(execute_graph, proxy.proxied_node) for proxy in uniqueproxies]
            concurrent.futures.wait(futures)
    else:
        # Run the graphs sequentially
        for p in uniqueproxies:
            execute_graph(p.proxied_node)

    return len(uniqueproxies)


def VariationsFor(actionproxy: ResultPtrProxy) -> ResultMapProxy:
    """
    Equivalent of ROOT.RDF.Experimental.VariationsFor in distributed mode.
    """
    # similar to resPtr.fActionPtr->MakeVariedAction()
    return actionproxy.create_variations()


def create_distributed_module(parentmodule):
    """
    Helper function to create the ROOT.RDF.Experimental.Distributed module.

    Users will see this module as the entry point of functions to create and
    run an RDataFrame computation distributedly.
    """
    distributed = types.ModuleType("ROOT.RDF.Experimental.Distributed")

    # PEP302 attributes
    distributed.__file__ = "<module ROOT.RDF.Experimental>"
    # distributed.__name__ is the constructor argument
    distributed.__path__ = []  # this makes it a package
    # distributed.__loader__ is not defined
    distributed.__package__ = parentmodule

    distributed = build_backends_submodules(distributed)

    # Inject top-level functions
    distributed.initialize = initialize
    distributed.RunGraphs = RunGraphs
    distributed.VariationsFor = VariationsFor

    return distributed
