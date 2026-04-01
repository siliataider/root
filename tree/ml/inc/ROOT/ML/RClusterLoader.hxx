// Author: Silia Taider, CERN 02/2026

/*************************************************************************
 * Copyright (C) 1995-2025, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_INTERNAL_ML_RCHUNKLOADER
#define ROOT_INTERNAL_ML_RCHUNKLOADER

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "ROOT/ML/RChunkConstructor.hxx"
#include "ROOT/ML/RFlat2DMatrix.hxx"
#include "ROOT/ML/RFlat2DMatrixOperators.hxx"
#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDF/Utils.hxx"

namespace ROOT::Experimental::Internal::ML {
/**
 * \struct RClusterRange
 */
struct RClusterRange {
   std::size_t rdfIdx;  // which rdf this cluster belongs to
   ULong64_t    start;    // first entry
   ULong64_t    end;      // one-past-last entry
   std::size_t numEntries{end - start}; // number of entries in the cluster

   std::size_t GetNumEntries() const { return numEntries; }
   void SetNumEntries(std::size_t num) { numEntries = num; }
};
   
/**
\class ROOT::Experimental::Internal::ML::RChunkLoaderFunctor

\brief Loading chunks made in RChunkLoader into tensors from data from RDataFrame.
*/

template <typename... ColTypes>
class RChunkLoaderFunctor {
   std::size_t fOffset{};
   std::size_t fVecSizeIdx{};
   float fVecPadding{};
   std::vector<std::size_t> fMaxVecSizes{};
   RFlat2DMatrix &fChunkTensor;

   std::size_t fNumChunkCols;

   int fI;
   int fNumColumns;

   //////////////////////////////////////////////////////////////////////////
   /// \brief Copy the content of a column into RTensor when the column consits of vectors
   template <typename T, std::enable_if_t<ROOT::Internal::RDF::IsDataContainer<T>::value, int> = 0>
   void AssignToTensor(const T &vec, int i, int numColumns)
   {
      std::size_t max_vec_size = fMaxVecSizes[fVecSizeIdx++];
      std::size_t vec_size = vec.size();

      float *dst = fChunkTensor.GetData() + fOffset + numColumns * i;
      if (vec_size < max_vec_size) // Padding vector column to max_vec_size with fVecPadding
      {
         std::copy(vec.begin(), vec.end(), dst);
         std::fill(dst + vec_size, dst + max_vec_size, fVecPadding);
      } else // Copy only max_vec_size length from vector column
      {
         std::copy(vec.begin(), vec.begin() + max_vec_size, dst);
      }
      fOffset += max_vec_size;
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Copy the content of a column into RTensor when the column consits of single values
   template <typename T, std::enable_if_t<!ROOT::Internal::RDF::IsDataContainer<T>::value, int> = 0>
   void AssignToTensor(const T &val, int i, int numColumns)
   {
      fChunkTensor.GetData()[fOffset + numColumns * i] = val;
      fOffset++;
      // fChunkTensor.GetData()[numColumns * i] = val;
   }

public:
   RChunkLoaderFunctor(RFlat2DMatrix &chunkTensor, std::size_t numColumns, const std::vector<std::size_t> &maxVecSizes,
                       float vecPadding, int i, std::size_t rowOffset = 0)
      : fChunkTensor(chunkTensor), fMaxVecSizes(maxVecSizes), fVecPadding(vecPadding), fI(i), fNumColumns(numColumns), fOffset(rowOffset * numColumns)
   {
   }

   void operator()(const ColTypes &...cols)
   {
      fVecSizeIdx = 0;
      (AssignToTensor(cols, fI, fNumColumns), ...);
   }
};

/**
\class ROOT::Experimental::Internal::ML::RClusterLoader

\brief Building and loading the chunks from the blocks and chunks constructed in RChunkConstructor

In this class the blocks are stiches together to form chunks that are loaded into memory. The blocks used to create each
chunk comes from different parts of the dataset. This is achieved by shuffling the blocks before distributing them into
chunks. The purpose of this process is to reduce bias during machine learning training by ensuring that the data is well
mixed. The dataset is also spit into training and validation sets with the user-defined validation split fraction.
*/

template <typename... Args>
class RClusterLoader {
private:
   std::vector<ROOT::RDF::RNode> &fRdfs;
   std::vector<std::size_t> fRdfSizes;
   std::vector<std::string> fCols;
   std::vector<std::size_t> fVecSizes;
   float fVecPadding;
   float fValidationSplit;
   bool fShuffle;
   std::size_t fSetSeed;

   std::size_t fNumCols;
   std::size_t fSumVecSizes;
   std::size_t fNumChunkCols;

   std::vector<RClusterRange> fAllClusters;
   std::vector<RClusterRange> fTrainingClusters;
   std::vector<RClusterRange> fValidationClusters;

   std::size_t fTotalEntries{0};
   std::size_t fNumTrainingEntries{0};
   std::size_t fNumValidationEntries{0};

   bool fIsFiltered{false};
   bool fSplitDiscovered{false};
   std::size_t fAccumulatedFilteredForTrain{0};

public:
RClusterLoader(std::vector<ROOT::RDF::RNode> &rdfs,
               const std::vector<std::string> &cols,
               const std::vector<std::size_t> &vecSizes,
               const float vecPadding,
               const float validationSplit,
               const bool shuffle,
               const std::size_t setSeed)
      : fRdfs(rdfs),
         fCols(cols),
         fVecSizes(vecSizes),
         fVecPadding(vecPadding),
         fValidationSplit(validationSplit),
         fShuffle(shuffle),
         fSetSeed(setSeed)
   {
      fNumCols     = fCols.size();
      fSumVecSizes = std::accumulate(fVecSizes.begin(), fVecSizes.end(), 0UL);
      fNumChunkCols = fNumCols + fSumVecSizes - fVecSizes.size();

      for (auto &rdf : fRdfs) {
         if (!rdf.GetFilterNames().empty()) {
            fIsFiltered = true;
            break;
         }
      }

      fRdfSizes.resize(fRdfs.size(), 0);

      // scan cluster boundaries across files
      for (std::size_t rdfIdx = 0; rdfIdx < fRdfs.size(); ++rdfIdx) {
         auto *lm = fRdfs[rdfIdx].GetLoopManager();
         const auto ranges = ROOT::Internal::RDF::GetClusterRanges(*lm);
         for (const auto &r : ranges)
         fAllClusters.push_back({rdfIdx, r.first, r.second});
      }

      for (const auto &c : fAllClusters) {
         auto numEntries = c.GetNumEntries();
         fTotalEntries += numEntries;
         fRdfSizes[c.rdfIdx] = numEntries;
      }
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Distribute the clusters into training and validation datasets
   void SplitDataset(std::size_t maxSize = 0)
   {
      if (fAllClusters.empty())
         throw std::runtime_error("RClusterLoader::SplitDataset: no clusters found.");

      if (fIsFiltered) {
         return;
      }

      if (fShuffle) {
         // --- Shuffled path
         // Every cluster contributes a prefix to training and a suffix to validation.
         // Cost: 2x I/O per epoch.
         for (const RClusterRange &c : fAllClusters) {
            const std::size_t sz = c.GetNumEntries();
            const std::size_t trainSz = static_cast<std::size_t>((1.0f - fValidationSplit) * sz);
            const std::size_t valSz   = sz - trainSz;

            if (trainSz > 0)
               { fTrainingClusters.push_back({c.rdfIdx, c.start, c.start + static_cast<ULong64_t>(trainSz)}); }
            if (valSz > 0)
               { fValidationClusters.push_back({c.rdfIdx, c.start + static_cast<ULong64_t>(trainSz), c.end}); }
         }
      } else {
         // --- Unshuffled path
         // Contiguous split: first (1 - validationSplit) fraction of entries go to
         // training, the remainder to validation. At most one cluster is split at
         // the boundary.
         const std::size_t targetTraining = fTotalEntries - static_cast<std::size_t>(fValidationSplit * fTotalEntries);

         std::size_t accumulated = 0;
         std::size_t splitIdx    = 0;
         for (std::size_t i = 0; i < fAllClusters.size(); ++i) {
            const std::size_t sz = fAllClusters[i].GetNumEntries();
            if (accumulated + sz <= targetTraining) {
               accumulated += sz;
               splitIdx = i + 1;
            } else {
               break;
            }
         }

         if (splitIdx < fAllClusters.size() && accumulated < targetTraining) {
            // Split the boundary cluster
            const RClusterRange &boundary = fAllClusters[splitIdx];
            const std::size_t    gap      = targetTraining - accumulated;

            fTrainingClusters.assign(fAllClusters.begin(), fAllClusters.begin() + splitIdx);
            fTrainingClusters.push_back({boundary.rdfIdx, boundary.start,
                                       boundary.start + static_cast<ULong64_t>(gap)});

            fValidationClusters.push_back({boundary.rdfIdx,
                                          boundary.start + static_cast<ULong64_t>(gap),
                                          boundary.end});
            fValidationClusters.insert(fValidationClusters.end(),
                                       fAllClusters.begin() + splitIdx + 1,
                                       fAllClusters.end());
         } else {
            fTrainingClusters.assign(fAllClusters.begin(),
                                    fAllClusters.begin() + splitIdx);
            fValidationClusters.assign(fAllClusters.begin() + splitIdx,
                                       fAllClusters.end());
         }
      }

      if (fTrainingClusters.empty())
         throw std::runtime_error(
            "RClusterLoader::SplitDataset: no entries for training after split. "
            "Reduce validation_split.");

      if (fValidationSplit > 0.0f && fValidationClusters.empty())
         throw std::runtime_error(
            "RClusterLoader::SplitDataset: no entries for validation after split. "
            "Increase validation_split.");

      fNumTrainingEntries = 0;
      fNumValidationEntries = 0;

      for (const auto &c : fTrainingClusters)  fNumTrainingEntries  += c.GetNumEntries();
      for (const auto &c : fValidationClusters) fNumValidationEntries += c.GetNumEntries();

      PrintClusterInfo("After SplitDataset");
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief shuffle the training cluster order for the upcoming epoch
   void ShuffleTrainingClusters(std::size_t epochIdx)
   {
      if (!fShuffle) return;

      std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed ^ epochIdx);
      std::shuffle(fTrainingClusters.begin(), fTrainingClusters.end(), g);

      PrintClusterInfo("After ShuffleTrainingClusters");
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Shuffle the validation cluster order for the upcoming epoch
   void ShuffleValidationClusters(std::size_t epochIdx)
   {
      if (!fShuffle) return;

      std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed ^ epochIdx);
      std::shuffle(fValidationClusters.begin(), fValidationClusters.end(), g);
   }

   void LoadClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx, const ULong64_t &startRow, const ULong64_t &endRow, std::size_t rowOffset = 0)
   {
      ROOT::RDF::RNode &rdf = fRdfs[rdfIdx];
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, startRow, endRow);
      RChunkLoaderFunctor<Args...> func(dest, fNumChunkCols, fVecSizes, fVecPadding, 0, rowOffset);
      rdf.Foreach(func, fCols);
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, 0, fRdfSizes[rdfIdx]);
   }

   //////////////////////////////////////////////////////////////////////////
    /// \brief Load one training cluster, returning the number of rows written.
    ///
    /// Filtered path, epoch 1 (!fSplitDiscovered):
    ///   - On the very first call, Count() is called across all RDFs to obtain
    ///     the total filtered entry count; fNumTrainingEntries and
    ///     fNumValidationEntries are set as targets immediately.
    ///   - A single Foreach on the full raw cluster range loads data AND captures
    ///     rdfentry_ simultaneously.  The real train/val boundary is computed from
    ///     the accumulated filtered count vs the target, then the train sub-range
    ///     is pushed to fTrainingClusters and the val sub-range directly to
    ///     fValidationClusters.
    ///   - Only the train rows are written into \p dest.
    ///
    /// All subsequent epochs (fSplitDiscovered == true): behaves exactly as the
    /// original LoadClusterInto — no overhead at all.
    std::size_t LoadTrainingClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx,
                                          ULong64_t startRow, ULong64_t endRow,
                                          std::size_t rowOffset = 0)
   {
      if (fIsFiltered && !fSplitDiscovered) {
         // Lazy initialisation: call Count() once to know the global filtered
         // totals and set the split targets before processing any cluster.
         if (fAccumulatedFilteredForTrain == 0 && fNumTrainingEntries == 0) {
            std::size_t totalFiltered = 0;
            for (auto &rdf : fRdfs)
               totalFiltered += *rdf.Count();
            fNumTrainingEntries   = static_cast<std::size_t>(totalFiltered * (1.0f - fValidationSplit));
            fNumValidationEntries = totalFiltered - fNumTrainingEntries;
            std::cout << "RClusterLoader: total filtered entries = " << totalFiltered
                      << ", training target = " << fNumTrainingEntries
                      << ", validation target = " << fNumValidationEntries
                      << "\n";
         }

         ROOT::RDF::RNode &rdf = fRdfs[rdfIdx];

         // Collect raw entry indices that pass the filter.
         std::vector<ULong64_t> rdfEntries;
         rdfEntries.reserve(endRow - startRow);

         // Single pass: load data into dest AND capture rdfentry_.
         RChunkLoaderFunctor<Args...> loader(dest, fNumChunkCols, fVecSizes, fVecPadding, 0, rowOffset);
         ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, startRow, endRow);

         std::vector<std::string> colsWithEntry;
         colsWithEntry.reserve(fCols.size() + 1);
         colsWithEntry.push_back("rdfentry_");
         colsWithEntry.insert(colsWithEntry.end(), fCols.begin(), fCols.end());

         std::cout << "RClusterLoader cols: ";
         for (const auto &c : fCols) {
            std::cout << c << " ";
         }

         std::cout << "RClusterLoader colsWithEntry: ";
         for (const auto &c : colsWithEntry) {
            std::cout << c << " ";
         }

         rdf.Foreach([&](ULong64_t entry, const Args &...cols) {
            rdfEntries.push_back(entry);
            loader(cols...);
         }, colsWithEntry);
         ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, 0, fRdfSizes[rdfIdx]);

         std::cout << "RClusterLoader: rdfentries values for cluster [" << rdfIdx << ":" << startRow << ", " << endRow
                   << ")  raw_count = " << endRow - startRow
                   << ", filtered_count = " << rdfEntries.size()
                   << "\n";
         for (const auto &e : rdfEntries) {
            std::cout << e << " ";
         }

         std::sort(rdfEntries.begin(), rdfEntries.end());

         const std::size_t totalFiltered = rdfEntries.size();
         if (totalFiltered == 0) {
            return 0;
         }

         const std::size_t trainRemaining = fNumTrainingEntries - fAccumulatedFilteredForTrain;
         const std::size_t trainCount = std::min(static_cast<std::size_t>(totalFiltered * (1.0f - fValidationSplit)), trainRemaining);
         const std::size_t valCount = totalFiltered - trainCount;

         // boundary: the raw entry index of the first entry NOT assigned to train.
         // Future epochs call ChangeBeginAndEndEntries(startRow, boundary) — stable
         // because the same filter always produces the same ordered entries.
         const ULong64_t boundary = (valCount > 0) ? rdfEntries[trainCount] : endRow;

         if (trainCount > 0)
            fTrainingClusters.push_back({rdfIdx, startRow, boundary, trainCount});
         if (valCount > 0)
            fValidationClusters.push_back({rdfIdx, boundary, endRow, valCount});

         fAccumulatedFilteredForTrain += trainCount;

         std::cout << "RClusterLoader: cluster [" << rdfIdx << ":" << startRow << ", " << endRow
                   << ")  raw_count = " << endRow - startRow
                   << ", filtered_count = " << totalFiltered
                   << ", train_count = " << trainCount
                   << ", val_count = " << valCount
                   << ", accumulated_for_train = " << fAccumulatedFilteredForTrain
                   << "\n";

         return trainCount;
      }
      LoadClusterInto(dest, rdfIdx, startRow, endRow, rowOffset);
      return endRow - startRow;
   }

   void FinaliseSplitDiscovery() { if (fIsFiltered) fSplitDiscovered = true; }

   bool IsSplitDiscovered() const { return !fIsFiltered || fSplitDiscovered; }
    
   void LoadValidationClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx, ULong64_t startRow, ULong64_t endRow, std::size_t rowOffset = 0)
   {
      LoadClusterInto(dest, rdfIdx, startRow, endRow, rowOffset);
   }

   //////////////////////////////////////////////////////////////////////////
   // Accessors
   std::size_t GetNumTrainingEntries()   const { return fNumTrainingEntries; }
   std::size_t GetNumValidationEntries() const { return fNumValidationEntries; }
   std::size_t GetNumChunkCols() const { return fNumChunkCols; }

   const std::vector<RClusterRange>& GetTrainingClusters() const
   {
      return (fIsFiltered && !fSplitDiscovered) ? fAllClusters : fTrainingClusters;
   }
   const std::vector<RClusterRange>& GetValidationClusters() const { return fValidationClusters; }
   
   std::size_t GetNumTrainingClusters() const
   {
      return (fIsFiltered && !fSplitDiscovered) ? fAllClusters.size() : fTrainingClusters.size();
   }
   std::size_t GetNumValidationClusters() const { return fValidationClusters.size(); }

   std::size_t GetNmTotalClusters() const { return fAllClusters.size(); }

    //////////////////////////////////////////////////////////////////////////

   // DBG
   void PrintClusterInfo(const std::string &label = "") const
   {
      if (!label.empty())
         std::cout << "\n=== " << label << " ===\n";

      std::cout << "Total clusters : " << fAllClusters.size()
               << "  (entries: ";
      std::size_t total = 0;
      for (const auto &c : fAllClusters) total += c.GetNumEntries();
      std::cout << total << ")\n";

      std::cout << "Training clusters  : " << fTrainingClusters.size()
               << "  (entries: " << fNumTrainingEntries << ")\n";
      for (std::size_t i = 0; i < fTrainingClusters.size(); ++i) {
         const auto &c = fTrainingClusters[i];
         std::cout << "  [" << i << "] rdf=" << c.rdfIdx
                  << "  entries=[" << c.start << ", " << c.end << ")"
                  << "  size=" << (c.GetNumEntries()) << "\n";
      }

      std::cout << "Validation clusters: " << fValidationClusters.size()
               << "  (entries: " << fNumValidationEntries << ")\n";
      for (std::size_t i = 0; i < fValidationClusters.size(); ++i) {
         const auto &c = fValidationClusters[i];
         std::cout << "  [" << i << "] rdf=" << c.rdfIdx
                  << "  entries=[" << c.start << ", " << c.end << ")"
                  << "  size=" << (c.GetNumEntries()) << "\n";
      }
      std::cout << std::flush;
   }
};

} // namespace ROOT::Experimental::Internal::ML
#endif // ROOT_INTERNAL_ML_RCHUNKLOADER
