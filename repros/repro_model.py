import torch


class HiggsClassifier(torch.nn.Module):
    def __init__(self, num_features: int, neuron: int):
        super(HiggsClassifier, self).__init__()

        self.model = torch.nn.Sequential(
            torch.nn.Linear(num_features, neuron),
            torch.nn.BatchNorm1d(neuron),
            torch.nn.ReLU(),
            torch.nn.Linear(neuron, neuron),
            torch.nn.ReLU(),
            torch.nn.BatchNorm1d(neuron),
            torch.nn.Linear(neuron, neuron),
            torch.nn.ReLU(),
            torch.nn.Linear(neuron, 1),
            torch.nn.Sigmoid(),
        )

    def forward(self, x):
        return self.model(x)
