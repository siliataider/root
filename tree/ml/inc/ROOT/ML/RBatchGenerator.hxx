// Author: Dante Niewenhuis, VU Amsterdam 07/2023
// Author: Kristupas Pranckietis, Vilnius University 05/2024
// Author: Nopphakorn Subsa-Ard, King Mongkut's University of Technology Thonburi (KMUTT) (TH) 08/2024
// Author: Vincenzo Eduardo Padulano, CERN 10/2024
// Author: Martin Føll, University of Oslo (UiO) & CERN 01/2026
// Author: Silia Taider, CERN 02/2026

/*************************************************************************
 * Copyright (C) 1995-2026, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_INTERNAL_ML_RBATCHGENERATOR
#define ROOT_INTERNAL_ML_RBATCHGENERATOR

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ROOT/ML/RBatchLoader.hxx"
#include "ROOT/ML/RClusterLoader.hxx"
#include "ROOT/ML/RDatasetLoader.hxx"
#include "ROOT/ML/RFlat2DMatrix.hxx"
#include "ROOT/ML/RFlat2DMatrixOperators.hxx"
#include "ROOT/ML/RSampler.hxx"
#include "ROOT/RDF/InterfaceUtils.hxx"

// Empty namespace to create a hook for the Pythonization
namespace ROOT::Experimental::ML {
}

namespace ROOT::Experimental::Internal::ML {
/**
 \class ROOT::Experimental::Internal::ML::RBatchGenerator
\brief

In this class, the processes of loading chunks (see RClusterLoader) and creating batches from those chunks (see
RBatchLoader) are combined, allowing batches from the training and validation sets to be loaded directly from a dataset
in an RDataFrame.
*/

template <typename... Args>
class RBatchGenerator {
private:
   std::vector<std::string> fCols;
   std::vector<std::size_t> fVecSizes;
   std::size_t fBatchSize;
   std::size_t fSetSeed;

   // cluster tables
   std::vector<RClusterRange> fClusterTable;
   std::vector<RClusterRange> fTrainingClusters;
   std::vector<RClusterRange> fValidationClusters;

   // buffer quantities
   std::size_t fBufferBatches;
   std::size_t fBufferCapacity;
   std::size_t fLowWatermark;
   std::size_t fHighWatermark;
   std::size_t fNumTotalEntries{0};

   std::size_t fTrainingClusterIdx{0};
   std::size_t fValidationClusterIdx{0};
   RFlat2DMatrix fTrainBuffer;
   RFlat2DMatrix fValidationBuffer;

   float fValidationSplit;

   std::unique_ptr<RDatasetLoader<Args...>> fDatasetLoader;
   std::unique_ptr<RClusterLoader<Args...>> fClusterLoader;
   std::unique_ptr<RBatchLoader> fTrainingBatchLoader;
   std::unique_ptr<RBatchLoader> fValidationBatchLoader;
   std::unique_ptr<RSampler> fTrainingSampler;
   std::unique_ptr<RSampler> fValidationSampler;

   std::unique_ptr<RFlat2DMatrixOperators> fTensorOperators;

   std::vector<ROOT::RDF::RNode> fRdfs;

   std::unique_ptr<std::thread> fLoadingThread;
   std::condition_variable fLoadingCondition;
   std::mutex fLoadingMutex;

   bool fDropRemainder;
   bool fShuffle;
   bool fLoadEager;
   std::string fSampleType;
   float fSampleRatio;
   bool fReplacement;

   bool fIsActive{false}; // Whether the loading thread is active
   // bool fUseWholeFile;

   bool fEpochActive{false};
   bool fTrainingEpochActive{false};
   bool fValidationEpochActive{false};

   std::size_t fNumTrainingEntries;
   std::size_t fNumValidationEntries;


   // flattened buffers for chunks and temporary tensors (rows * cols)
   std::vector<RFlat2DMatrix> fTrainingDatasets;
   std::vector<RFlat2DMatrix> fValidationDatasets;

   RFlat2DMatrix fTrainingDataset;
   RFlat2DMatrix fValidationDataset;

   RFlat2DMatrix fSampledTrainingDataset;
   RFlat2DMatrix fSampledValidationDataset;

public:
   RBatchGenerator(const std::vector<ROOT::RDF::RNode> &rdfs,
                  const std::size_t batchSize,
                  const std::size_t bufferBatches,
                  const std::vector<std::string> &cols,
                  const std::vector<std::size_t> &vecSizes = {},
                  const float vecPadding = 0.0,
                  const float validationSplit = 0.0,
                  bool shuffle = true,
                  bool dropRemainder = true,
                  const std::size_t setSeed = 0,
                  bool loadEager = false,
                  std::string sampleType = "",
                  float sampleRatio = 1.0,
                  bool replacement = false)
      :  fRdfs(rdfs),
         fCols(cols),
         fVecSizes(vecSizes),
         fBatchSize(batchSize),
         fBufferBatches(bufferBatches),
         fValidationSplit(validationSplit),
         fDropRemainder(dropRemainder),
         fSetSeed(setSeed),
         fShuffle(shuffle),
         fLoadEager(loadEager),
         fSampleType(sampleType),
         fSampleRatio(sampleRatio),
         fReplacement(replacement)
   {
      fTensorOperators = std::make_unique<RFlat2DMatrixOperators>(fShuffle, fSetSeed);

      if (fLoadEager) {
         fDatasetLoader = std::make_unique<RDatasetLoader<Args...>>(fRdfs, fValidationSplit, fCols, fVecSizes, vecPadding, fShuffle, fSetSeed);
         fDatasetLoader->SplitDatasets();

         if (fSampleType == "") {
            fDatasetLoader->ConcatenateDatasets();
            fTrainingDataset   = fDatasetLoader->GetTrainingDataset();
            fValidationDataset = fDatasetLoader->GetValidationDataset();
            fNumTrainingEntries   = fDatasetLoader->GetNumTrainingEntries();
            fNumValidationEntries = fDatasetLoader->GetNumValidationEntries();
         } else {
            fTrainingDatasets   = fDatasetLoader->GetTrainingDatasets();
            fValidationDatasets = fDatasetLoader->GetValidationDatasets();
            fTrainingSampler = std::make_unique<RSampler>(fTrainingDatasets, fSampleType, fSampleRatio, fReplacement, fShuffle, fSetSeed);
            fValidationSampler = std::make_unique<RSampler>(fValidationDatasets, fSampleType, fSampleRatio, fReplacement, fShuffle, fSetSeed);
            fNumTrainingEntries   = fTrainingSampler->GetNumEntries();
            fNumValidationEntries = fValidationSampler->GetNumEntries();
         }

      } else {
         // scan cluster metadata
         fClusterLoader = std::make_unique<RClusterLoader<Args...>>(fRdfs, fCols, fVecSizes, vecPadding, fValidationSplit, fShuffle, fSetSeed);

         // split cluster list into training and validation
         fClusterLoader->SplitDataset();

         fClusterLoader->PrintClusterInfo("All clusters");

         fNumTrainingEntries   = fClusterLoader->GetNumTrainingEntries();
         fNumValidationEntries = fClusterLoader->GetNumValidationEntries();

         // derive buffer quantities
         fBufferCapacity = fBatchSize * fBufferBatches;
         fLowWatermark   = fBufferCapacity / 2;
         fHighWatermark  = fBufferCapacity;

         // std::cout << "Total training entries: " << fNumTrainingEntries << "\n"
         //           << "Total validation entries: " << fNumValidationEntries << "\n"
         //           << "Buffer capacity (entries): " << fBufferCapacity << "\n"
         //           << "Low watermark (entries): " << fLowWatermark << "\n"
         //           << "High watermark (entries): " << fHighWatermark << "\n";
      }

      // batch loaders
      fTrainingBatchLoader = std::make_unique<RBatchLoader>(fBatchSize, fCols, fLoadingMutex, fLoadingCondition, fVecSizes, fNumTrainingEntries, fDropRemainder);
      fValidationBatchLoader = std::make_unique<RBatchLoader>(fBatchSize, fCols, fLoadingMutex, fLoadingCondition, fVecSizes, fNumValidationEntries, fDropRemainder);
   }

   ~RBatchGenerator() { DeActivate(); }

   void DeActivate()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         if (!fIsActive)
            return;
         fIsActive = false;
      }

      fLoadingCondition.notify_all();

      if (fLoadingThread) {
         if (fLoadingThread->joinable()) {
            fLoadingThread->join();
         }
      }

      fLoadingThread.reset();
   }

   /// \brief Activate the loading process by spawning the loading thread.
   void Activate()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         if (fIsActive)
            return;

         fIsActive = true;
      }

      if (fLoadEager) {
         return;
      }

      fLoadingThread = std::make_unique<std::thread>(&RBatchGenerator::LoadData, this);
   }

   /// \brief Activate the training epoch by starting the batchloader.
   void ActivateTrainingEpoch()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         fTrainingEpochActive = true;
         fTrainingClusterIdx = 0;
      }

      fTrainingBatchLoader->Activate();
      fLoadingCondition.notify_all();
   }

   void DeActivateTrainingEpoch()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         fTrainingEpochActive = false;
      }

      fTrainingBatchLoader->Reset();
      fTrainingBatchLoader->DeActivate();
      fLoadingCondition.notify_all();
   }

   void ActivateValidationEpoch()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         fValidationEpochActive = true;
         fValidationClusterIdx = 0;
      }

      fValidationBatchLoader->Activate();
      fLoadingCondition.notify_all();
   }

   void DeActivateValidationEpoch()
   {
      {
         std::lock_guard<std::mutex> lock(fLoadingMutex);
         fValidationEpochActive = false;
      }

      fValidationBatchLoader->Reset();
      fValidationBatchLoader->DeActivate();
      fLoadingCondition.notify_all();
   }

   /// \brief Main loop for loading chunks and creating batches.
   /// The producer (loading thread) will keep loading chunks and creating batches until the end of the epoch is
   /// reached, or the generator is deactivated.
   void LoadData()
   {
      // Set minimum number of batches to keep in the queue before producer goes to work.
      // This is to ensure that the producer will get a chance to work if the consumer is too fast and drains the queue
      // quickly.
      std::unique_lock<std::mutex> lock(fLoadingMutex);

      const std::size_t numTrainingClusters = fClusterLoader->GetNumTrainingClusters();
      const std::size_t numValidationClusters = fClusterLoader->GetNumValidationClusters();

      while (true) {
         // Wait until we have work or shutdown
         fLoadingCondition.wait(lock, [&] {
            return !fIsActive || (fTrainingEpochActive && fTrainingClusterIdx < numTrainingClusters) ||
                   (fValidationEpochActive && fValidationClusterIdx < numValidationClusters);
         });

         if (!fIsActive)
            break;

         // Helper: check if validation queue below watermark and needs the producer
         auto validationEmpty = [&] {
            if (!fValidationEpochActive || fValidationClusterIdx >= numValidationClusters)
               return false;
            if (fValidationBatchLoader->isProducerDone())
               return false;
            return fValidationBatchLoader->GetNumBatchQueue() < fLowWatermark / fBatchSize;
         };

         // -- TRAINING --
         if (fTrainingEpochActive) {
            while (true) {
               // Stop conditions (shutdown or epoch end)
               if (!fIsActive || !fTrainingEpochActive)
                  break;

               // No more chunks to load: signal consumers
               if (fTrainingClusterIdx >= numTrainingClusters) {
                  // flush whatever remains in the buffer
                  if (fTrainBuffer.GetRows() > 0) {
                     lock.unlock();
                     RFlat2DMatrix fShuffledTrainBuffer;
                     fTensorOperators->ShuffleTensor(fShuffledTrainBuffer, fTrainBuffer);
                     fTrainingBatchLoader->CreateBatches(fShuffledTrainBuffer, /*isLastBatch=*/true);
                     fTrainBuffer = RFlat2DMatrix{};   // clear buffer
                     lock.lock();
                  }
                  fTrainingBatchLoader->MarkProducerDone();
                  break;
               }

               // In the case of training prefetching, we could start requesting data for the next training loop while
               // validation is active and might need data. To avoid getting stuck in the training loop, we check if the
               // validation queue is below watermark and if so, we break out of the training loop.
               if (validationEmpty()) {
                  break;
               }

               // If queue is not empty, wait until it drains below watermark, or validation needs data, or we are
               // deactivated.
               if (fTrainingBatchLoader->GetNumBatchQueue() >= fLowWatermark / fBatchSize) {
                  fLoadingCondition.wait(lock, [&] {
                     return !fIsActive || !fTrainingEpochActive ||
                            fTrainingBatchLoader->GetNumBatchQueue() < fLowWatermark / fBatchSize || validationEmpty();
                  });
                  continue;
               }

               // Claim cluster under lock
               const std::size_t clusterIdx = fTrainingClusterIdx++;
               const bool isLastCluster = (clusterIdx == numTrainingClusters - 1);

               // Release lock while reading and loading data to allow the consumer to access the queue freely in
               // parallel. The loading thread re-acquires the lock in CreateBatches when it needs to push batches to
               // the queue.
               lock.unlock();

               // load whole cluster
               RFlat2DMatrix clusterTensor;
               fClusterLoader->LoadTrainingCluster(clusterTensor, clusterIdx);
               
               // append cluster rows to buffer
               fTrainBuffer.Append(clusterTensor);

               // std::cout << "Buffer now has " << fTrainBuffer.GetRows() << " rows and " << fTrainBuffer.GetCols() << " cols\n";
   
               // buffer full enough or last cluster: shuffle rows and push batches
               if (fTrainBuffer.GetRows() >= fHighWatermark || isLastCluster) {
                  RFlat2DMatrix fShuffledTrainBuffer;
                  fTensorOperators->ShuffleTensor(fShuffledTrainBuffer, fTrainBuffer);
                  fTrainingBatchLoader->CreateBatches(fShuffledTrainBuffer, isLastCluster);
                  fTrainBuffer = RFlat2DMatrix{};   // clear buffer after batching
               }
   
               lock.lock();
            }
         }

         // -- VALIDATION --
         if (fValidationEpochActive) {
            while (true) {
               // Stop conditions (shutdown or epoch end)
               if (!fIsActive || !fValidationEpochActive)
                  break;

               // No more chunks to load: signal consumers
               if (fValidationClusterIdx >= numValidationClusters) {
                  if (fValidationBuffer.GetRows() > 0) {
                     lock.unlock();
                     fValidationBatchLoader->CreateBatches(fValidationBuffer, /*isLastBatch=*/true);
                     fValidationBuffer = RFlat2DMatrix{};
                     lock.lock();
                  }
                  fValidationBatchLoader->MarkProducerDone();
                  break;
               }

               // If queue is not hungry, wait until it drains below watermark, or we are deactivated
               if (fValidationBatchLoader->GetNumBatchQueue() >= fLowWatermark / fBatchSize) {
                  fLoadingCondition.wait(lock, [&] {
                     return !fIsActive || !fValidationEpochActive
                         || fValidationBatchLoader->GetNumBatchQueue() < fLowWatermark / fBatchSize;
                  });
                  continue;
               }

               // Claim chunk under lock
               const std::size_t clusterIdx = fValidationClusterIdx++;
               const bool isLastCluster = (fValidationClusterIdx >= numValidationClusters);

               lock.unlock();

               RFlat2DMatrix clusterTensor;
               fClusterLoader->LoadValidationCluster(clusterTensor, clusterIdx);
               fValidationBuffer.Append(clusterTensor);

               if (fValidationBuffer.GetRows() >= fHighWatermark || isLastCluster) {
                  fValidationBatchLoader->CreateBatches(fValidationBuffer, isLastCluster);
                  fValidationBuffer = RFlat2DMatrix{};
               }

               lock.lock();
            }
         }
      }
   }

   /// \brief Create training batches by first loading a chunk (see RClusterLoader) and split it into batches (see
   /// RBatchLoader)
   void CreateTrainBatches()
   {
      fTrainingBatchLoader->Activate();

      if (fLoadEager) {
         if (fSampleType == "") {
            fTensorOperators->ShuffleTensor(fSampledTrainingDataset, fTrainingDataset);
         }

         else {
            fTrainingSampler->Sampler(fSampledTrainingDataset);
         }

         fTrainingBatchLoader->CreateBatches(fSampledTrainingDataset, true);
         fTrainingBatchLoader->MarkProducerDone();

      } else {
         fClusterLoader->ShuffleTrainingClusters();
      }
   }

   /// \brief Creates validation batches by first loading a chunk (see RClusterLoader), and then split it into batches
   /// (see RBatchLoader)
   void CreateValidationBatches()
   {
      fValidationBatchLoader->Activate();

      if (fLoadEager) {
         if (fSampleType == "") {
            fTensorOperators->ShuffleTensor(fSampledValidationDataset, fValidationDataset);
         }

         else {
            fValidationSampler->Sampler(fSampledValidationDataset);
         }

         fValidationBatchLoader->CreateBatches(fSampledValidationDataset, true);
         fValidationBatchLoader->MarkProducerDone();
      }

      else {
         fClusterLoader->ShuffleValidationClusters();
      }
   }

   /// \brief Loads a training batch from the queue
   RFlat2DMatrix GetTrainBatch()
   {
      // Get next batch if available
      return fTrainingBatchLoader->GetBatch();
   }

   /// \brief Loads a validation batch from the queue
   RFlat2DMatrix GetValidationBatch()
   {
      // Get next batch if available
      return fValidationBatchLoader->GetBatch();
   }

   std::size_t NumberOfTrainingBatches() { return fTrainingBatchLoader->GetNumBatches(); }
   std::size_t NumberOfValidationBatches() { return fValidationBatchLoader->GetNumBatches(); }

   std::size_t TrainRemainderRows() { return fTrainingBatchLoader->GetNumRemainderRows(); }
   std::size_t ValidationRemainderRows() { return fValidationBatchLoader->GetNumRemainderRows(); }

   bool IsActive()
   {
      std::lock_guard<std::mutex> lock(fLoadingMutex);
      return fIsActive;
   }

   bool IsTrainingActive()
   {
      std::lock_guard<std::mutex> lock(fLoadingMutex);
      return fTrainingEpochActive;
   }

   bool IsValidationActive()
   {
      std::lock_guard<std::mutex> lock(fLoadingMutex);
      return fValidationEpochActive;
   }
};

} // namespace ROOT::Experimental::Internal::ML

#endif // ROOT_INTERNAL_ML_RBATCHGENERATOR