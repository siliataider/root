import ROOT
import torch
from tqdm import tqdm
from repro_model import HiggsClassifier

df = ROOT.RDataFrame("tree", "root://eosuser.cern.ch//eos/user/s/staider/ROOT_files/df106_HiggsToFourLeptons_train.root")
print("liste des colonnes:", df.GetColumnNames())

def is_higgs(category: str) -> int:
    return 1 if category == "higgs" else 0

df = df.Define("isHiggsRef", is_higgs, ["sample_category"])

n_higgs = df.Sum("isHiggsRef").GetValue()
total_events = df.Count().GetValue()
print(f"pourcentage de higgs: {((n_higgs / total_events) * 100):.2f} %  ( {n_higgs} / {total_events} ) ")

batch_size = 1000
drop_remainder = True
validation_split = 0.3
# TODO try out avec jet
columns = ['m4l', 'good_lep', 'goodlep_E', 'goodlep_eta', 'goodlep_phi', 'goodlep_pt', 'goodlep_type', 'isHiggsRef']
target = 'isHiggsRef'
max_vec_sizes = {'good_lep': 4, 'goodlep_E': 4, 'goodlep_eta': 4, 'goodlep_phi': 4, 'goodlep_pt': 4, 'goodlep_type': 4}
chunk_size = 100000
block_size = 10000
shuffle = True
set_seed = 42

train, validation = ROOT.TMVA.Experimental.CreatePyTorchGenerators(
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

num_features = len(train.train_columns)
model = HiggsClassifier(num_features=num_features, neuron=40)
loss_fn = torch.nn.BCELoss(reduction="mean")
optimizer = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.9)

epochs = 50
for epoch in range(epochs):
    # training
    model.train()
    total_loss = 0

    for i, (x_train, y_train) in enumerate(tqdm(train, desc=f"Training epoch {epoch+1}/{epochs}")):
        outputs = model(x_train)
        loss = loss_fn(outputs, y_train)
        
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

    print(f"Loss: {total_loss/(i+1):.4f}")

# validation
model.eval()
val_loss = 0
val_correct = 0
val_total = 0

with torch.no_grad():
    for j, (x_val, y_val) in enumerate(validation):
        outputs = model(x_val)
        loss = loss_fn(outputs, y_val)
        val_loss += loss.item()
        
        preds = (outputs > 0.5).float()
        val_correct += (preds == y_val).sum().item()
        val_total += y_val.size(0)

avg_val_loss = val_loss / (j + 1)
val_accuracy = val_correct / val_total

print(f"Validation Loss: {avg_val_loss:.4f}  Accuracy: {val_accuracy:.4f}\n")


torch.save(model.state_dict(), "higgs_classifier.pth")