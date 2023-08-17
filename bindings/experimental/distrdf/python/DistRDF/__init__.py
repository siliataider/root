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

from typing import List, Dict, Optional, Callable, Any
import ast
import inspect
import warnings
from functools import singledispatch

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


def is_valid_plot_object(obj):
    """
    Checks if the object is a valid plot object for live visualization.

    Args:
        obj: The object to be checked.

    Returns:
        bool: True if the object is a valid plot object for live visualization, False otherwise.
    """
    import ROOT

    ALLOWED_OPERATIONS = ["Histo1C", "Histo1D", "Histo1F", "Histo1I", "Histo1K", "Histo1S",
                          "Histo2C", "Histo2D", "Histo2F", "Histo2I", "Histo2S",
                          "Histo3C", "Histo3D", "Histo3F", "Histo3I", "Histo3S",
                          "Graph",
                          "Profile1D", "Profile2D"]

    try:
        if obj.proxied_node.operation.name in ALLOWED_OPERATIONS:
            return True
        
    except:
        return False


@singledispatch   
def live_visualize(plot_object_callback_dict: Dict[type, Optional[Callable]], global_callback: Optional[Callable] = None):
    """
    Enables live visualization for the given plot objects by setting the
    by setting the live_plot_object_callback_dict attribute of the Headnode.

    Args:
        live_plot_object_callback_dict (Dict[type, Optional[Callable]]): A dictionary where the keys are
        the plot objects and the values are the corresponding callback functions. The callback
        functions are optional (can be set to None) if no callback is required for a specific object.
    """

    # Import the necessary ROOT classes inside the function to avoid circular dependency
    import ROOT
    from DistRDF import HeadNode

    value = list(plot_object_callback_dict)[0].proxied_node.value

    if value:
        warnings.warn("live_visualize() should be called before triggering the computation graph. Skipping live visualization.")

    else:
        plot_object_callback_id_dict = {}
        for hist, callback in plot_object_callback_dict.items():
            callbacks = []

            if not is_valid_plot_object(hist):
                raise ValueError("All elements in the list must be derived from ROOT's TH1 class. Skipping live visualization.")            
            
            if callback:
                if callable(callback):
                    if len(inspect.signature(callback).parameters) == 1:
                        if is_callback_safe(callback):
                            callbacks.append(callback)
                        else:
                            callback = None
                            warnings.warn("The provided callback function contains blocked actions. Skipping callback: ")
                    else:
                        callback = None
                        warnings.warn("The callback function should have exactly one parameter. Skipping callback.")
                else:
                    callback = None
                    warnings.warn("The provided callback is not callable. Skipping callback.")
            
            if global_callback:
                callbacks.append(global_callback)

            plot_object_callback_id_dict[hist.proxied_node.node_id] = callbacks

        headnode = list(plot_object_callback_dict)[0].proxied_node.get_head() # Assuming all hists share the same headnode
        headnode.plot_object_callback_id_dict = plot_object_callback_id_dict
    

@live_visualize.register(list)
def _1(plot_objects: List, callback: Optional[Callable] = None):

    # Handle the case where the user passes a list without a dictionary
    if callback is None:
        plot_object_callback_dict = {hist: None for hist in plot_objects}
    else:
        plot_object_callback_dict = {hist: callback for hist in plot_objects}

    # Call the main live_visualize function with the plot_object_callback_dict
    live_visualize(plot_object_callback_dict)
    

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
    if not proxies:
        logger.warning("RunGraphs: Got an empty list of handles, now quitting.")
        return 0

    # Get proxies belonging to distinct computation graphs
    uniqueproxies = list({proxy.proxied_node.get_head(): proxy for proxy in proxies}.values())

    # Submit all computation graphs concurrently from multiple Python threads.
    # The submission is not computationally intensive
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(uniqueproxies)) as executor:
        futures = [executor.submit(execute_graph, proxy.proxied_node) for proxy in uniqueproxies]
        concurrent.futures.wait(futures)

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
