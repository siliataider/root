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
from functools import singledispatch


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


@singledispatch   
def LiveVisualize(drawable_callback_dict: Dict[type, Optional[Callable]], global_callback: Optional[Callable] = None):
    """
    Enables real-time data representation for the given drawable objects by setting the
    drawables_dict attribute of the Headnode.

    Args:
        drawable_callback_dict (Dict[type, Optional[Callable]]): A dictionary where 
            keys are drawable objects and values are the corresponding callback functions. 
            The callback functions are optional (can be set to None).

        global_callback (Optional[Callable]): An optional global callback function that 
            is applied to all drawable objects.

    Raises:
        ValueError: If a passed drawable object is not valid.
    """
    # Check if the objects already have a value (the computation graph has already been triggered)
    if is_value_set(list(drawable_callback_dict)[0]):
        warnings.warn("LiveVisualize() should be called before triggering the computation graph. \
                      Skipping live visualization.")
        return

    drawable_id_callback_dict = {
        # Key: node_id of the drawable object's proxied_node
        # Value: List of validated callback functions for the drawable object
        obj.proxied_node.node_id: process_callbacks(callback, global_callback)
        for obj, callback in drawable_callback_dict.items()
        # Filter: Only include valid drawable objects
        if is_valid_drawable(obj)
    }

    # Assuming all objects share the same headnode
    headnode = list(drawable_callback_dict)[0].proxied_node.get_head()  
    headnode.drawables_dict = drawable_id_callback_dict


@LiveVisualize.register(list)
def _1(drawables: List, callback: Optional[Callable] = None):
    """
    Wrapper function to facilitate calling LiveVisualize with a list of drawable objects.

    Args:
        drawables (List): A list of drawable objects to visualize.
		
        callback (Optional[Callable]): An optional callback function to be applied to the drawable objects.

    Notes:
        This function constructs a dictionary of drawable objects and their associated callback functions,
        and then calls the main LiveVisualize function with the constructed dictionary.
    """
    if callback is None:
        drawable_callback_dict = {obj: None for obj in drawables}
    else:
        drawable_callback_dict = {obj: callback for obj in drawables}

    LiveVisualize(drawable_callback_dict)


def is_value_set(obj):
    """
    Checks if the value of an object is set.

    Args:
        obj: The object to be checked.

    Returns:
        bool: True if the value of the object is set, False otherwise.
    """
    if obj.proxied_node.value:
        return True
    else:
        return False
    

def process_callbacks(callback, global_callback):
    """
    Process and validate callback functions for a drawable object.

    Args:
        obj: The drawable object to be processed.
		
        callback: The callback function associated with the drawable object.
		
        global_callback: The global callback function applied to all drawable objects.

    Returns:
        List[Callable]: A list of validated callback functions.
    """
    callbacks = []
    if callback is None:
        pass 
    elif not callable(callback):
        warnings.warn("The provided callback is not callable. Skipping callback.")
    else:
        if len(inspect.signature(callback).parameters) != 1:
            warnings.warn("The callback function should have exactly one parameter. \
                          Skipping callback.")
        elif not is_callback_safe(callback):
            warnings.warn("The provided callback function contains blocked actions. \
                          Skipping callback.")
        else:
            callbacks.append(callback)

    if global_callback:
        callbacks.append(global_callback)

    return callbacks


def is_callback_safe(callback):
    """
    Checks if the provided callback function is safe for live visualization, 
        (does not contain blocked actions).

    Args:
        callback (function): The callback function to check.

    Returns:
        bool: True if the callback function is safe, 
            False otherwise.
    """
    # Get the source code of the callback function
    callback_source = inspect.getsource(callback)
    # Parse the callback function's source code
    callback_source_ast = ast.parse(callback_source)
    for node in ast.walk(callback_source_ast):
        if is_action_blocked(node):
            return False
        
    return True


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
    # Check if this node is a function
    if isinstance(node, ast.Call):
        # Check if we're calling an attribute of an object
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
            if func_name in BLOCKED_ACTIONS:
                return True
            
    return False


def is_valid_drawable(obj):
    """
    Checks if the object is a valid drawable object for live visualization.

    Args:
        obj: The object to be checked.

    Returns:
        bool: True if the object is a valid drawable object for live visualization 
            according to the ALLOWED_OPERATIONS list , False otherwise.
    """
    ALLOWED_OPERATIONS = ["Histo1D", "Histo2D", "Histo3D",
                          "Graph", "GraphAsymmErrors",
                          "Profile1D", "Profile2D"]
    try:    
        if obj.proxied_node.operation.name in ALLOWED_OPERATIONS:
            return True    
    except:
        raise ValueError("All elements in the list must be derived from ROOT's TH1 class. \
                         Skipping live visualization.")