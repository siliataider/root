# Author: Dante Niewenhuis, VU Amsterdam 07/2023
# Author: Kristupas Pranckietis, Vilnius University 05/2024
# Author: Nopphakorn Subsa-Ard, King Mongkut's University of Technology Thonburi (KMUTT) (TH) 08/2024
# Author: Vincenzo Eduardo Padulano, CERN 10/2024
# Author: Martin Føll, University of Oslo (UiO) & CERN 01/2026
# Author: Silia Taider, CERN 02/2026

################################################################################
# Copyright (C) 1995-2026, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################

from __future__ import annotations

import atexit
from typing import TYPE_CHECKING, Any, Callable, Tuple

if TYPE_CHECKING:
    import numpy as np
    import tensorflow as tf
    import torch

    import ROOT


class _RDataLoader:
    def get_template(
        self,
        x_rdf: ROOT.RDF.RNode,
        columns: list[str] | None = None,
        max_vec_sizes: dict[str, int] | None = None,
    ) -> Tuple[str, list[int]]:
        """
        Generate a template for the DataLoader based on the given
        RDataFrame and columns.

        Args:
            x_rdf (RNode): RDataFrame or RNode object.
            columns (list[str], optional): Columns that should be loaded.
                                 Defaults to loading all columns
                                 in the given RDataFrame
            max_vec_sizes (dict[str, int], optional):
                                 Mapping from vector column name
                                 to the maximum size of the vector.
                                 Required when using vector based columns.

        Returns:
            Tuple[str, list[int]]: Template string for the DataLoader and list of max vector sizes
        """
        if not columns:
            columns = x_rdf.GetColumnNames()
        if max_vec_sizes is None:
            max_vec_sizes = {}

        template_string = ""

        self.given_columns = []
        self.all_columns = []

        max_vec_sizes_list = []

        for name in columns:
            name_str = str(name)
            self.given_columns.append(name_str)
            column_type = x_rdf.GetColumnType(name_str)
            template_string = f"{template_string}{column_type},"

            if "RVec" in column_type:
                # Add column for each element if column is a vector
                if name_str in max_vec_sizes:
                    max_vec_sizes_list.append(max_vec_sizes[name_str])
                    for i in range(max_vec_sizes[name_str]):
                        self.all_columns.append(f"{name_str}_{i}")

                else:
                    raise ValueError(
                        f"No max size given for feature {name_str}. \
                        Given max sizes: {max_vec_sizes}"
                    )

            else:
                self.all_columns.append(name_str)

        return template_string[:-1], max_vec_sizes_list

    def __init__(
        self,
        rdataframes: ROOT.RDF.RNode | list[ROOT.RDF.RNode] | None = None,
        batch_size: int = 0,
        approx_batches_in_memory: int = 1,
        columns: list[str] | None = None,
        max_vec_sizes: dict[str, int] | None = None,
        vec_padding: int = 0,
        target: str | list[str] | None = None,
        weights: str = "",
        validation_split: float = 0.0,
        max_chunks: int = 0,
        shuffle: bool = True,
        drop_remainder: bool = True,
        set_seed: int = 0,
        load_eager: bool = False,
        sampling_type: str = "",
        sampling_ratio: float = 1.0,
        replacement: bool = False,
    ) -> None:
        """Wrapper around the C++ DataLoader

        Args:
            rdataframes (ROOT.RDF.RNode | list[ROOT.RDF.RNode] | None):
                RDataFrame or list of RDataFrames to load from.
            batch_size (int):
                Number of entries per batch returned by the generator.
            approx_batches_in_memory (int):
                Approximate number of batches that should be kept in memory at
                the same time. Higher value results in faster loading, but
                also higher memory usage. Defaults to 1.
            columns (list[str] | None):
                Names of columns to load. If not given, all columns are used.
            max_vec_sizes (dict[str, int] | None):
                Mapping from vector column name to the maximum size of the vector.
                Required when using vector based columns.
            vec_padding (int):
                Value used to pad vectors with if the vector is smaller
                than the given max vector length. Defaults to 0.
            target (str | list[str] | None):
                Name or list of names of target column(s).
            weights (str):
                Column used to weight events.
                Can only be used when a target is given.
            validation_split (float):
                The ratio of batches being kept for validation.
                Value has to be between 0 and 1. Defaults to 0.0.
            max_chunks (int):
                The number of chunks that should be loaded for an epoch.
                If not given, the whole file is used.
            shuffle (bool):
                Batches consist of random events and are shuffled every epoch.
                Defaults to True.
            drop_remainder (bool):
                Drop the remainder of data that is too small to compose full batch.
                Defaults to True.
            set_seed (int):
                For reproducibility: Set the seed for the random number generator used
                to split the dataset into training and validation and shuffling of the chunks
                Defaults to 0 which means that the seed is set to the random device.
            load_eager (bool):
                If True, load the full dataset(s) into memory.
                If False, load data lazily in chunks. Defaults to False.
            sampling_type (str):
                Describes the mode of sampling from the minority and majority dataframes.
                Supported values are ``"undersampling"`` and ``"oversampling"``. Requires ``load_eager=True``.
                Defaults to ``""``.
                For 'undersampling' and 'oversampling' it requires a list of exactly two dataframes as input,
                where the dataframe with the most entries is the majority dataframe
                and the dataframe with the fewest entries is the minority dataframe.
            sampling_ratio (float):
                Ratio of minority and majority entries in the resampled dataset.
                Requires ``load_eager=True`` and ``sampling_type="undersampling"`` or ``"oversampling"``. Defaults to 1.0.
            replacement (bool):
                Whether the sampling is with (True) or without (False) replacement.
                Requires ``load_eager=True`` and ``sampling_type="undersampling"``. Defaults to False.
        """

        from ROOT import RDF

        if rdataframes is None:
            rdataframes = []
        if columns is None:
            columns = []
        if max_vec_sizes is None:
            max_vec_sizes = {}
        if target is None or target == "":
            target = []

        if validation_split < 0.0 or validation_split > 1.0:
            raise ValueError(
                f"The validation_split has to be in range [0.0, 1.0] \n \
                    given value is {validation_split}"
            )

        if not hasattr(rdataframes, "__iter__"):
            rdataframes = [rdataframes]
        self.noded_rdfs = [RDF.AsRNode(rdf) for rdf in rdataframes]

        if isinstance(target, str):
            target = [target]

        self.target_columns = target
        self.weights_column = weights

        template, max_vec_sizes_list = self.get_template(self.noded_rdfs[0], columns, max_vec_sizes)

        self.num_columns = len(self.all_columns)
        self.batch_size = batch_size

        # Handle target
        self.target_given = len(self.target_columns) > 0
        self.weights_given = len(self.weights_column) > 0
        if self.target_given:
            for target in self.target_columns:
                if target not in self.all_columns:
                    raise ValueError(
                        f"Provided target not in given columns: \ntarget => \
                            {target}\ncolumns => {self.all_columns}"
                    )

            self.target_indices = [self.all_columns.index(target) for target in self.target_columns]

            # Handle weights
            if self.weights_given:
                if weights in self.all_columns:
                    self.weights_index = self.all_columns.index(self.weights_column)
                    self.train_indices = [
                        c for c in range(len(self.all_columns)) if c not in self.target_indices + [self.weights_index]
                    ]
                else:
                    raise ValueError(
                        f"Provided weights not in given columns: \nweights => \
                            {weights}\ncolumns => {self.all_columns}"
                    )
            else:
                self.train_indices = [c for c in range(len(self.all_columns)) if c not in self.target_indices]

        elif self.weights_given:
            raise ValueError("Weights can only be used when a target is provided")
        else:
            self.train_indices = [c for c in range(len(self.all_columns))]

        self.train_columns = [c for c in self.all_columns if c not in self.target_columns + [self.weights_column]]

        import ROOT

        # The DataLoader will create a separate C++ thread for I/O.
        # Enable thread safety in ROOT from here, to make sure there is no
        # interference between the main Python thread (which might call into
        # cling via cppyy) and the I/O thread.
        ROOT.EnableThreadSafety()

        self.data_loader = ROOT.Experimental.Internal.ML.RDataLoader(template)(
            self.noded_rdfs,
            batch_size,
            approx_batches_in_memory,
            self.given_columns,
            max_vec_sizes_list,
            vec_padding,
            validation_split,
            shuffle,
            drop_remainder,
            set_seed,
            load_eager,
            sampling_type,
            sampling_ratio,
            replacement,
        )

        atexit.register(self.DeActivate)

    @property
    def isActive(self):
        return self.data_loader.IsActive()

    def isTrainingActive(self):
        return self.data_loader.IsTrainingActive()

    def isValidationActive(self):
        return self.data_loader.IsValidationActive()

    def Activate(self):
        """Initialize the generator to be used for a loop, this spawns the loading thread"""
        self.data_loader.Activate()

    def DeActivate(self):
        """Deactivate the generator"""
        self.data_loader.DeActivate()

    def ActivateTrainingEpoch(self):
        """Activate the training epoch of the generator"""
        self.data_loader.ActivateTrainingEpoch()

    def ActivateValidationEpoch(self):
        """Activate the validation epoch of the generator"""
        self.data_loader.ActivateValidationEpoch()

    def DeActivateTrainingEpoch(self):
        """Deactivate the training epoch of the generator"""
        self.data_loader.DeActivateTrainingEpoch()

    def DeActivateValidationEpoch(self):
        """Deactivate the validation epoch of the generator"""
        self.data_loader.DeActivateValidationEpoch()

    def CreateTrainBatches(self):
        """Create the first training batches from the first chunk"""
        self.data_loader.CreateTrainBatches()

    def CreateValidationBatches(self):
        """Create the first validation batches from the first chunk"""
        self.data_loader.CreateValidationBatches()

    @property
    def num_training_batches(self) -> int:
        return self.data_loader.NumberOfTrainingBatches()

    @property
    def num_validation_batches(self) -> int:
        return self.data_loader.NumberOfValidationBatches()

    @property
    def train_remainder_rows(self) -> int:
        return self.data_loader.TrainRemainderRows()

    @property
    def val_remainder_rows(self) -> int:
        return self.data_loader.ValidationRemainderRows()

    def GetSample(self):
        """
        Return a sample of data that has the same size and types as the actual
        result. This sample can be used to define the shape and size of the
        output

        Returns:
            np.ndarray: data sample
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError("Failed to import numpy needed for the ML dataloader")

        # Split the target and weight
        if not self.target_given:
            return np.zeros((self.batch_size, self.num_columns))

        if not self.weights_given:
            if len(self.target_indices) == 1:
                return np.zeros((self.batch_size, self.num_columns - 1)), np.zeros((self.batch_size)).reshape(-1, 1)

            return np.zeros((self.batch_size, self.num_columns - 1)), np.zeros(
                (self.batch_size, len(self.target_indices))
            )

        if len(self.target_indices) == 1:
            return (
                np.zeros((self.batch_size, self.num_columns - 2)),
                np.zeros((self.batch_size)).reshape(-1, 1),
                np.zeros((self.batch_size)).reshape(-1, 1),
            )

        return (
            np.zeros((self.batch_size, self.num_columns - 2)),
            np.zeros((self.batch_size, len(self.target_indices))),
            np.zeros((self.batch_size)).reshape(-1, 1),
        )

    def ConvertBatchToNumpy(self, batch) -> np.ndarray:
        """Convert a RTensor into a NumPy array

        Args:
            batch (RTensor): Batch returned from the DataLoader

        Returns:
            np.ndarray: converted batch
        """
        try:
            import numpy as np
        except ImportError:
            raise ImportError("Failed to import numpy needed for the ML dataloader")

        data = batch.GetData()
        batch_size, num_columns = tuple(batch.GetShape())

        data.reshape((batch_size * num_columns,))

        return_data = np.asarray(data).reshape(batch_size, num_columns)

        # Splice target column from the data if target is given
        if self.target_given:
            train_data = return_data[:, self.train_indices]
            target_data = return_data[:, self.target_indices]

            # Splice weight column from the data if weight is given
            if self.weights_given:
                weights_data = return_data[:, self.weights_index]

                if len(self.target_indices) == 1:
                    return train_data, target_data.reshape(-1, 1), weights_data.reshape(-1, 1)

                return train_data, target_data, weights_data.reshape(-1, 1)

            if len(self.target_indices) == 1:
                return train_data, target_data.reshape(-1, 1)

            return train_data, target_data

        return return_data

    def ConvertBatchToPyTorch(self, batch: Any) -> torch.Tensor:
        """Convert a RTensor into a PyTorch tensor

        Args:
            batch (RTensor): Batch returned from the DataLoader

        Returns:
            torch.Tensor: converted batch
        """
        import numpy as np
        import torch

        data = batch.GetData()
        batch_size, num_columns = tuple(batch.GetShape())

        data.reshape((batch_size * num_columns,))

        return_data = torch.as_tensor(np.asarray(data)).reshape(batch_size, num_columns)

        # Splice target column from the data if target is given
        if self.target_given:
            train_data = return_data[:, self.train_indices]
            target_data = return_data[:, self.target_indices]

            # Splice weight column from the data if weight is given
            if self.weights_given:
                weights_data = return_data[:, self.weights_index]

                if len(self.target_indices) == 1:
                    return train_data, target_data.reshape(-1, 1), weights_data.reshape(-1, 1)

                return train_data, target_data, weights_data.reshape(-1, 1)

            if len(self.target_indices) == 1:
                return train_data, target_data.reshape(-1, 1)

            return train_data, target_data

        return return_data

    def ConvertBatchToTF(self, batch: Any) -> Any:
        """
        Convert a RTensor into a TensorFlow tensor

        Args:
            batch (RTensor): Batch returned from the DataLoader

        Returns:
            tensorflow.Tensor: converted batch
        """
        import tensorflow as tf

        data = batch.GetData()
        batch_size, num_columns = tuple(batch.GetShape())

        data.reshape((batch_size * num_columns,))

        return_data = tf.constant(data, shape=(batch_size, num_columns))

        if batch_size != self.batch_size:
            return_data = tf.pad(return_data, tf.constant([[0, self.batch_size - batch_size], [0, 0]]))

        # Splice target column from the data if weight is given
        if self.target_given:
            train_data = tf.gather(return_data, indices=self.train_indices, axis=1)
            target_data = tf.gather(return_data, indices=self.target_indices, axis=1)

            # Splice weight column from the data if weight is given
            if self.weights_given:
                weights_data = tf.gather(return_data, indices=[self.weights_index], axis=1)

                return train_data, target_data, weights_data

            return train_data, target_data

        return return_data

    # Return a batch when available
    def GetTrainBatch(self) -> Any:
        """Return the next training batch of data from the given RDataFrame

        Returns:
            (np.ndarray): Batch of data of size.
        """

        batch = self.data_loader.GetTrainBatch()
        return batch if (batch and batch.GetSize() > 0) else None

    def GetValidationBatch(self) -> Any:
        """Return the next training batch of data from the given RDataFrame

        Returns:
            (np.ndarray): Batch of data of size.
        """

        batch = self.data_loader.GetValidationBatch()
        return batch if (batch and batch.GetSize() > 0) else None


# context managers for the loading thread
class _TrainingEpochContext:
    def __init__(self, data_loader: _RDataLoader):
        self._data_loader = data_loader
        data_loader.Activate()
        data_loader.CreateTrainBatches()

    def __enter__(self):
        self._data_loader.ActivateTrainingEpoch()
        return self

    def __exit__(self, *_):
        self._data_loader.DeActivateTrainingEpoch()


class _ValidationEpochContext:
    def __init__(self, data_loader: _RDataLoader):
        self._data_loader = data_loader
        data_loader.Activate()
        data_loader.CreateValidationBatches()

    def __enter__(self):
        self._data_loader.ActivateValidationEpoch()
        return self

    def __exit__(self, *_):
        self._data_loader.DeActivateValidationEpoch()


# formatted iterator (returned by AsTorch / AsNumpy / AsTensorFlow)
class _FormattedLoader:
    """
    Iterable that drives the C++ loading thread for one epoch and converts
    each batch to the requested format. Returned by the AsTorch / AsNumpy /
    AsTensorFlow methods on RBatchDataset.
    """

    def __init__(
        self,
        data_loader: _RDataLoader,
        conversion_fn: Callable,
        is_training: bool,
    ):
        self._data_loader = data_loader
        self._conversion_fn = conversion_fn
        self._is_training = is_training
        self._gen = None

    def _make_gen(self):
        ctx_cls = _TrainingEpochContext if self._is_training else _ValidationEpochContext
        get_batch = self._data_loader.GetTrainBatch if self._is_training else self._data_loader.GetValidationBatch

        with ctx_cls(self._data_loader):
            while True:
                batch = get_batch()
                if batch is None:
                    break
                yield self._conversion_fn(batch)

    def __iter__(self):
        return self._make_gen()


class RBatchDataset:
    """
    Represents one split (training or validation) of an RDataLoader.
    Call AsTorch(), AsNumpy(), or AsTensorFlow() to get an iterable over
    batches in the desired format.

    Example::

        train, val = ROOT.Experimental.ML.RDataLoader(df, batch_size=1000, ...)

        for x, y in train.AsTorch():
            ...

        for x, y in val.AsNumpy():
            ...
    """

    def __init__(self, data_loader: _RDataLoader, is_training: bool):
        self._data_loader = data_loader
        self._is_training = is_training

    # ---- metadata ----

    @property
    def columns(self) -> list[str]:
        """All column names as they appear in each batch tensor."""
        return self._data_loader.all_columns

    @property
    def train_columns(self) -> list[str]:
        """Feature column names (columns minus target and weights)."""
        return self._data_loader.train_columns

    @property
    def target_columns(self) -> list[str]:
        """Target column names."""
        return self._data_loader.target_columns

    @property
    def weights_column(self) -> str:
        """Weights column name, or empty string if not set."""
        return self._data_loader.weights_column

    @property
    def number_of_batches(self) -> int:
        """Total number of batches in this split for one epoch."""
        if self._is_training:
            return self._data_loader.num_training_batches
        return self._data_loader.num_validation_batches

    @property
    def last_batch_no_of_rows(self) -> int:
        """Number of rows in the last (remainder) batch, 0 if no remainder."""
        if self._is_training:
            return self._data_loader.train_remainder_rows
        return self._data_loader.val_remainder_rows

    # ---- format methods ----

    def AsNumpy(self) -> _FormattedLoader:
        """
        Return an iterable that yields batches as NumPy arrays.

        Yields:
            np.ndarray, or (np.ndarray, np.ndarray) when a target is set,
            or (np.ndarray, np.ndarray, np.ndarray) when weights are also set.
        """
        return _FormattedLoader(self._data_loader, self._data_loader.ConvertBatchToNumpy, self._is_training)

    def AsTorch(self, device: str | None = None) -> _FormattedLoader:
        """
        Return an iterable that yields batches as PyTorch tensors.

        Args:
            device: Optional torch device string, e.g. "cuda" or "cpu".
                    If None, tensors are returned on the default device.

        Yields:
            torch.Tensor, or (torch.Tensor, torch.Tensor) when a target is set,
            or (torch.Tensor, torch.Tensor, torch.Tensor) when weights are also set.
        """
        if device is None:
            conversion_fn = self._data_loader.ConvertBatchToPyTorch
        else:
            def conversion_fn(batch):
                result = self._data_loader.ConvertBatchToPyTorch(batch)
                if isinstance(result, tuple):
                    return tuple(t.to(device) for t in result)
                return result.to(device)

        return _FormattedLoader(self._data_loader, conversion_fn, self._is_training)

    def AsTensorFlow(self) -> tf.data.Dataset:
        """
        Return a tf.data.Dataset over batches as TensorFlow tensors.

        Returns:
            tf.data.Dataset yielding tf.Tensor, or tuples of tf.Tensor when
            a target (and optionally weights) is set.
        """
        import tensorflow as tf

        batch_size = self._data_loader.batch_size
        num_train_cols = len(self._data_loader.train_columns)
        num_target_cols = len(self._data_loader.target_columns)

        if not self._data_loader.target_given:
            signature = tf.TensorSpec(shape=(batch_size, num_train_cols), dtype=tf.float32)
        elif not self._data_loader.weights_given:
            signature = (
                tf.TensorSpec(shape=(batch_size, num_train_cols), dtype=tf.float32),
                tf.TensorSpec(shape=(batch_size, num_target_cols), dtype=tf.float32),
            )
        else:
            signature = (
                tf.TensorSpec(shape=(batch_size, num_train_cols), dtype=tf.float32),
                tf.TensorSpec(shape=(batch_size, num_target_cols), dtype=tf.float32),
                tf.TensorSpec(shape=(batch_size, 1), dtype=tf.float32),
            )

        loader = _FormattedLoader(self._data_loader, self._data_loader.ConvertBatchToTF, self._is_training)
        return tf.data.Dataset.from_generator(lambda: loader, output_signature=signature)


# ---------------------------------------------------------------------------
# Public: RDataLoader
# ---------------------------------------------------------------------------

def RDataLoader(
    rdataframes: ROOT.RDF.RNode | list[ROOT.RDF.RNode] | None = None,
    batch_size: int = 0,
    approx_batches_in_memory: int = 10,
    columns: list[str] | None = None,
    max_vec_sizes: dict[str, int] | None = None,
    vec_padding: float = 0.0,
    target: str | list[str] | None = None,
    weights: str = "",
    validation_split: float = 0.0,
    shuffle: bool = True,
    drop_remainder: bool = True,
    set_seed: int = 0,
    load_eager: bool = False,
    sampling_type: str = "",
    sampling_ratio: float = 1.0,
    replacement: bool = False,
) -> Tuple[RBatchDataset, RBatchDataset | None]:
    """
    Create a data loader for ML training from a ROOT RDataFrame.

    Always returns a (train, val) tuple. When validation_split is 0, val is None.

    Args:
        rdataframes:
            RDataFrame or list of RDataFrames to load from.
        batch_size:
            Number of entries per batch.
        approx_batches_in_memory:
            Approximate number of batches held in the shuffle buffer at any
            time. Larger values improve shuffle quality across cluster
            boundaries at the cost of higher memory usage. Acts as a soft
            cap: the buffer may temporarily exceed this by up to one
            cluster's worth of rows. Defaults to 10.
        columns:
            Names of columns to load. If not given, all columns are used.
        max_vec_sizes:
            Maximum size per vector column. Required for RVec columns.
        vec_padding:
            Padding value for vectors shorter than their max size. Defaults to 0.
        target:
            Name or list of names of target column(s).
        weights:
            Column to use for event weighting. Requires a target.
        validation_split:
            Fraction of data to reserve for validation, between 0 and 1.
            Defaults to 0.0.
        shuffle:
            Whether to shuffle data across cluster boundaries every epoch.
            Defaults to True.
        drop_remainder:
            Drop the last batch if smaller than batch_size. Defaults to True.
        set_seed:
            Seed for the random number generator. 0 means a random seed is
            drawn from the system. Defaults to 0.
        load_eager:
            If True, load the full dataset into memory before training.
            If False (default), load lazily in chunks.
        sampling_type:
            Resampling strategy: "undersampling" or "oversampling".
            Requires load_eager=True and exactly two input dataframes.
        sampling_ratio:
            Ratio of minority to majority entries in the resampled dataset.
            Requires load_eager=True and sampling_type set.
        replacement:
            Whether undersampling is with replacement. Requires load_eager=True
            and sampling_type="undersampling".

    Returns:
        Tuple (train, val) where train is an RBatchDataset and val is
        either an RBatchDataset or None when validation_split is 0.

    Example::

        train, val = ROOT.Experimental.ML.RDataLoader(
            df,
            batch_size=1000,
            approx_batches_in_memory=50,
            columns=["x", "y", "label"],
            target="label",
            validation_split=0.2,
        )

        for epoch in range(10):
            for x, y in train.AsTorch(device="cuda"):
                ...
            for x, y in val.AsTorch(device="cuda"):
                ...
    """
    if rdataframes is None:
        rdataframes = []
    if columns is None:
        columns = []
    if max_vec_sizes is None:
        max_vec_sizes = {}
    if target is None or target == "":
        target = []
    if isinstance(target, str):
        target = [target]

    if validation_split < 0.0 or validation_split > 1.0:
        raise ValueError(
            f"validation_split must be in [0.0, 1.0], got {validation_split}"
        )

    data_loader = _RDataLoader(
        rdataframes=rdataframes,
        batch_size=batch_size,
        approx_batches_in_memory=approx_batches_in_memory,
        columns=columns,
        max_vec_sizes=max_vec_sizes,
        vec_padding=vec_padding,
        target=target,
        weights=weights,
        validation_split=validation_split,
        shuffle=shuffle,
        drop_remainder=drop_remainder,
        set_seed=set_seed,
        load_eager=load_eager,
        sampling_type=sampling_type,
        sampling_ratio=sampling_ratio,
        replacement=replacement,
    )

    train = RBatchDataset(data_loader, is_training=True)
    val = RBatchDataset(data_loader, is_training=False) if validation_split > 0.0 else None

    return train, val


# ---------------------------------------------------------------------------
# Injection hook
# ---------------------------------------------------------------------------

def _inject_dataloader_api(parentmodule):
    """
    Inject the public Python API into the ROOT.Experimental.ML namespace.
    Only RDataLoader and RBatchDataset are part of the public surface.
    """
    for obj in [RDataLoader, RBatchDataset]:
        setattr(parentmodule, obj.__name__, obj)