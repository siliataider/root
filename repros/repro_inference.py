import ROOT
import torch
import numpy as np
import time
from repro_model import HiggsClassifier

def log_time(label, start):
    print(f"[Time] {label}: {time.time() - start:.2f} s")

start_load = time.time()

df = ROOT.RDataFrame('tree', "df106_HiggsToFourLeptons_train.root")

def is_higgs(category: str) -> int:
    return 1 if category == "higgs" else 0

df = df.Define("isHiggsRef", is_higgs, ["sample_category"])

n_elements = df.Count().GetValue()

batch_size = 1
drop_remainder = True
validation_split = 0
columns = ['m4l', 'good_lep', 'goodlep_E', 'goodlep_eta', 'goodlep_phi', 'goodlep_pt', 'goodlep_type', 'isHiggsRef']
target = 'isHiggsRef'
max_vec_sizes = {'good_lep': 4, 'goodlep_E': 4, 'goodlep_eta': 4, 'goodlep_phi': 4, 'goodlep_pt': 4, 'goodlep_type': 4}
chunk_size = n_elements
block_size = n_elements
shuffle = False
set_seed = 42

inference = ROOT.TMVA.Experimental.CreatePyTorchGenerators(
    df,
    batch_size=batch_size,
    drop_remainder=drop_remainder,
    validation_split=validation_split,
    columns=columns,
    target=target,
    max_vec_sizes=max_vec_sizes,
    chunk_size=chunk_size,
    block_size=block_size,
    shuffle=shuffle,
    set_seed=set_seed
)

num_features = len(inference.train_columns)
model = HiggsClassifier(num_features=num_features, neuron=40)
model.load_state_dict(torch.load("higgs_classifier.pth"))
model.eval()  

generator_list = list(inference)

log_time("Loading", start_load)
start_infer = time.time()

def pred_is_higgs(_rdfentry: int) -> float:
    x_inference, _ = generator_list[_rdfentry]
    with torch.no_grad():
        output = model(x_inference)
    return float(output.item())

THRESHOLD = 0.5
def is_over_threshold(x: float) -> int:
    return 1 if x > THRESHOLD else 0

df = df.Define("isHiggsPred", pred_is_higgs, ["rdfentry_"])
df = df.Define("isHiggsPredClass", is_over_threshold, ["isHiggsPred"])

y_true = df.AsNumpy(columns=["isHiggsRef"])["isHiggsRef"]
y_pred = df.AsNumpy(columns=["isHiggsPredClass"])["isHiggsPredClass"]

log_time("Inference", start_infer)
log_time("Total", start_load)

tp = np.sum((y_true == 1) & (y_pred == 1))
tn = np.sum((y_true == 0) & (y_pred == 0))
fp = np.sum((y_true == 0) & (y_pred == 1))
fn = np.sum((y_true == 1) & (y_pred == 0))

accuracy = (tp + tn) / len(y_true)
precision = tp / (tp + fp)
recall = tp / (tp + fn)
f1 = 2 * (precision * recall) / (precision + recall)

print(f"TP: {tp}, TN: {tn}, FP: {fp}, FN: {fn}")
print(f"Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}, F1: {f1:.4f}")
