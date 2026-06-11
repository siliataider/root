"""
Benchmark: C++ expression vs Numba vs Pure Python callbacks in RDataFrame
"""

import time
import math
import numpy as np
import ROOT

# Configuration
N_EVENTS = [10_000_000]
N_RUNS = 5
N_WARMUP = 2  # Warmup runs (discarded from timing)
THREAD_CONFIGS = [1]

# Setup: Create a test file with random data
def create_test_file(n_events, filename="benchmark_data.root"):
    rdf = ROOT.RDataFrame(n_events)
    rdf = (rdf
        .Define("E", "gRandom->Gaus(100, 10)")
        .Define("px", "gRandom->Gaus(0, 30)")
        .Define("py", "gRandom->Gaus(0, 30)")
        .Define("pz", "gRandom->Gaus(0, 30)")
    )
    rdf.Snapshot("Events", filename, ["E", "px", "py", "pz"])
    return filename

# Approach 1: Pure C++ expression
def benchmark_cpp_expression(filename, n_runs=N_RUNS, n_warmup=N_WARMUP):
    warmup_times = []
    times = []
    
    # Warmup runs
    for _ in range(n_warmup):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", "sqrt(E*E - px*px - py*py - pz*pz)")
        
        start = time.perf_counter()
        result = rdf.Sum("mass").GetValue()
        end = time.perf_counter()
        
        warmup_times.append(end - start)
    
    # Timed runs
    for _ in range(n_runs):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", "sqrt(E*E - px*px - py*py - pz*pz)")
        
        start = time.perf_counter()
        result = rdf.Sum("mass").GetValue()
        end = time.perf_counter()
        
        times.append(end - start)
    
    return {
        "mean": np.mean(times),
        "std": np.std(times),
        "times": times,
        "warmup_times": warmup_times,
        "warmup_mean": np.mean(warmup_times),
        "warmup_std": np.std(warmup_times),
    }

# Approach 2: Numba JIT
@ROOT.Numba.Declare(["float", "float", "float", "float"], "float")
def get_mass_numba(E, px, py, pz):
    return math.sqrt(E**2 - (px**2 + py**2 + pz**2))

def benchmark_numba(filename, n_runs=N_RUNS, n_warmup=N_WARMUP):
    warmup_times = []
    times = []
    
    # Warmup runs
    for _ in range(n_warmup):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", "Numba::get_mass_numba(E, px, py, pz)")
        
        start = time.perf_counter()
        result = rdf.Sum("mass").GetValue()
        end = time.perf_counter()
        
        warmup_times.append(end - start)
    
    # Timed runs
    for _ in range(n_runs):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", "Numba::get_mass_numba(E, px, py, pz)")
        
        start = time.perf_counter()
        result = rdf.Sum("mass").GetValue()
        end = time.perf_counter()
        
        times.append(end - start)
    
    return {
        "mean": np.mean(times),
        "std": np.std(times),
        "times": times,
        "warmup_times": warmup_times,
        "warmup_mean": np.mean(warmup_times),
        "warmup_std": np.std(warmup_times),
    }

# Approach 3: Pure Python callback
def get_mass_python(E: float, px: float, py: float, pz: float) -> float:
    m2 = E*E - (px*px + py*py + pz*pz)
    return math.sqrt(max(m2, 0.0))

def benchmark_pure_python(filename, n_runs=N_RUNS, n_warmup=N_WARMUP):
    warmup_times = []
    times = []
    
    # Warmup runs
    for _ in range(n_warmup):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", get_mass_python, ["E", "px", "py", "pz"])
        
        
        start = time.perf_counter()
        result = rdf.Sum("mass")
        ROOT.Internal.RDF.TriggerRun.__release_gil__ = True
        ROOT.Internal.RDF.TriggerRun(ROOT.RDF.AsRNode(rdf))
        end = time.perf_counter()
        
        warmup_times.append(end - start)
    
    # Timed runs
    for _ in range(n_runs):
        rdf = ROOT.RDataFrame("Events", filename)
        rdf = rdf.Define("mass", get_mass_python, ["E", "px", "py", "pz"])
        
        start = time.perf_counter()
        result = rdf.Sum("mass").GetValue()
        end = time.perf_counter()
        
        times.append(end - start)
    
    return {
        "mean": np.mean(times),
        "std": np.std(times),
        "times": times,
        "warmup_times": warmup_times,
        "warmup_mean": np.mean(warmup_times),
        "warmup_std": np.std(warmup_times),
    }

# Run benchmarks
def run_all_benchmarks(n_threads, skip_numba=False, skip_python=False, skip_cpp=False):
    ROOT.DisableImplicitMT()

    if n_threads > 1:
        ROOT.EnableImplicitMT(n_threads)

    print(f"\nRunning with {n_threads} thread(s)")

    results = {
        "n_threads": n_threads,
        "n_events": [],
        "n_runs": N_RUNS,
        "n_warmup": N_WARMUP,
        # C++ results
        "cpp_mean": [], "cpp_std": [], "cpp_times": [],
        "cpp_warmup_mean": [], "cpp_warmup_std": [], "cpp_warmup_times": [],
        # Numba results
        "numba_mean": [], "numba_std": [], "numba_times": [],
        "numba_warmup_mean": [], "numba_warmup_std": [], "numba_warmup_times": [],
        # Python results
        "python_mean": [], "python_std": [], "python_times": [],
        "python_warmup_mean": [], "python_warmup_std": [], "python_warmup_times": [],
        "cpp_per_event_us": [],
        "numba_per_event_us": [],
        "python_per_event_us": [],
    }
    
    for n_events in N_EVENTS:
        print(f"\n{'='*60}")
        print(f"Benchmarking with {n_events:,} events")
        print('='*60)
        
        filename = create_test_file(n_events)
        
        if not skip_cpp:
            print("  C++ expression...", end=" ", flush=True)
            cpp_result = benchmark_cpp_expression(filename)
            print(f"{cpp_result['mean']*1000:.2f} ± {cpp_result['std']*1000:.2f} ms")
            print(f"    (warmup: {cpp_result['warmup_mean']*1000:.2f} ± {cpp_result['warmup_std']*1000:.2f} ms)")
        else:
            cpp_result = {
                "mean": 0, "std": 0, "times": [],
                "warmup_mean": 0, "warmup_std": 0, "warmup_times": []
            }
        
        if not skip_numba:
            print("  Numba JIT...", end=" ", flush=True)
            numba_result = benchmark_numba(filename)
            print(f"{numba_result['mean']*1000:.2f} ± {numba_result['std']*1000:.2f} ms")
            print(f"    (warmup: {numba_result['warmup_mean']*1000:.2f} ± {numba_result['warmup_std']*1000:.2f} ms)")
        else:
            numba_result = {
                "mean": 0, "std": 0, "times": [],
                "warmup_mean": 0, "warmup_std": 0, "warmup_times": []
            }
        
        if not skip_python:
            print("  Pure Python...", end=" ", flush=True)
            python_result = benchmark_pure_python(filename)
            print(f"{python_result['mean']*1000:.2f} ± {python_result['std']*1000:.2f} ms")
            print(f"    (warmup: {python_result['warmup_mean']*1000:.2f} ± {python_result['warmup_std']*1000:.2f} ms)")
        else:
            python_result = {
                "mean": 0, "std": 0, "times": [],
                "warmup_mean": 0, "warmup_std": 0, "warmup_times": []
            }

        results["n_events"].append(n_events)
        
        results["cpp_mean"].append(cpp_result["mean"])
        results["cpp_std"].append(cpp_result["std"])
        results["cpp_times"].append(cpp_result["times"])
        results["cpp_warmup_mean"].append(cpp_result["warmup_mean"])
        results["cpp_warmup_std"].append(cpp_result["warmup_std"])
        results["cpp_warmup_times"].append(cpp_result["warmup_times"])
        results["cpp_per_event_us"].append(cpp_result["mean"] * 1e6 / n_events)
        
        results["numba_mean"].append(numba_result["mean"])
        results["numba_std"].append(numba_result["std"])
        results["numba_times"].append(numba_result["times"])
        results["numba_warmup_mean"].append(numba_result["warmup_mean"])
        results["numba_warmup_std"].append(numba_result["warmup_std"])
        results["numba_warmup_times"].append(numba_result["warmup_times"])
        if numba_result["mean"] > 0:
            results["numba_per_event_us"].append(numba_result["mean"] * 1e6 / n_events)
        else:
            results["numba_per_event_us"].append(0)
        
        results["python_mean"].append(python_result["mean"])
        results["python_std"].append(python_result["std"])
        results["python_times"].append(python_result["times"])
        results["python_warmup_mean"].append(python_result["warmup_mean"])
        results["python_warmup_std"].append(python_result["warmup_std"])
        results["python_warmup_times"].append(python_result["warmup_times"])
        results["python_per_event_us"].append(python_result["mean"] * 1e6 / n_events)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark RDataFrame callbacks')
    parser.add_argument('--skip-numba', action='store_true', help='Skip Numba benchmark')
    parser.add_argument('--skip-python', action='store_true', help='Skip Python benchmark')
    parser.add_argument('--skip-cpp', action='store_true', help='Skip cpp benchmark')
    args = parser.parse_args()
    
    print("RDataFrame Callback Benchmark")
    print("="*60)
    print(f"ROOT version: {ROOT.gROOT.GetVersion()}")
    print(f"Event counts: {N_EVENTS}")
    print(f"Runs per benchmark: {N_RUNS}")
    print(f"Warmup runs: {N_WARMUP}")
    if args.skip_numba:
        print("Numba benchmark: SKIPPED")
    
    results = []

    for n_threads in THREAD_CONFIGS:
        print("\n" + "="*60)
        print(f"THREAD CONFIG: {n_threads}")
        print("="*60)

        res = run_all_benchmarks(
            n_threads,
            skip_numba=args.skip_numba,
            skip_python=args.skip_python,
            skip_cpp=args.skip_cpp
        )
        results.append(res)

    import json
    with open("benchmark_results_mt.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nRaw results saved to benchmark_results_mt.json")

"""
# set numba_jit=True
python bench_pure_python_mt.py --skip-python

# set numba_jit=False
python bench_pure_python_mt.py --skip-numba --skip-cpp
"""