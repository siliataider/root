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
   std::size_t numEntries() const { return end - start; }
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
                       float vecPadding, int i)
      : fChunkTensor(chunkTensor), fMaxVecSizes(maxVecSizes), fVecPadding(vecPadding), fI(i), fNumColumns(numColumns)
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

   std::vector<std::pair<std::size_t, std::size_t>> fTrainingClusterRanges;
   std::vector<std::pair<std::size_t, std::size_t>> fValidationClusterRanges;

   std::size_t fTotalEntries{0};
   std::size_t fNumTrainingEntries{0};
   std::size_t fNumValidationEntries{0};

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

      // scan cluster boundaries across files
      for (std::size_t rdfIdx = 0; rdfIdx < fRdfs.size(); ++rdfIdx) {
         auto *lm = fRdfs[rdfIdx].GetLoopManager();
         const auto ranges = ROOT::Internal::RDF::GetClusterRanges(*lm);
         for (const auto &r : ranges)
         fAllClusters.push_back({rdfIdx, r.first, r.second});
      }

      for (const auto &c : fAllClusters) {
         fTotalEntries += c.numEntries();
      }
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Distribute the clusters into training and validation datasets
   void SplitDataset(std::size_t maxSize = 0)
   {
      if (fAllClusters.empty())
         throw std::runtime_error("RClusterLoader::SplitDataset: no clusters found.");
   
      if (fShuffle) {
         std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
         std::shuffle(fAllClusters.begin(), fAllClusters.end(), g);
      }
   
      const std::size_t targetTraining = fTotalEntries - static_cast<std::size_t>(fValidationSplit * fTotalEntries);
   
      // fill training with whole clusters
      std::size_t accumulated = 0;
      std::size_t splitIdx = 0;
      for (std::size_t i = 0; i < fAllClusters.size(); ++i) {
         const std::size_t sz = fAllClusters[i].numEntries();
         if (accumulated + sz <= targetTraining) {
            accumulated += sz;
            splitIdx = i + 1;
         } else {
            break;
         }
      }
   
      // handle the boundary cluster if exact split is needed
      // splitIdx points at the first cluster that would overflow training
      // If accumulated < targetTraining, split that cluster at the boundary entry
      if (splitIdx < fAllClusters.size() && accumulated < targetTraining) {
         const RClusterRange &boundary = fAllClusters[splitIdx];
         const std::size_t gap = targetTraining - accumulated;
   
         // training gets [start, start+gap), validation gets [start+gap, end)
         RClusterRange trainPart  = {boundary.rdfIdx, boundary.start, boundary.start + static_cast<ULong64_t>(gap)};
         RClusterRange validPart  = {boundary.rdfIdx, boundary.start + static_cast<ULong64_t>(gap), boundary.end};
   
         // training: all whole clusters before splitIdx + the train part
         fTrainingClusters.assign(fAllClusters.begin(), fAllClusters.begin() + splitIdx);
         fTrainingClusters.push_back(trainPart);
   
         // validation: the validation part + all whole clusters after splitIdx
         fValidationClusters.push_back(validPart);
         fValidationClusters.insert(fValidationClusters.end(), fAllClusters.begin() + splitIdx + 1, fAllClusters.end());
      } else {
         // no splitting needed
         fTrainingClusters.assign(fAllClusters.begin(), fAllClusters.begin() + splitIdx);
         fValidationClusters.assign(fAllClusters.begin() + splitIdx, fAllClusters.end());
      }
   
      if (fTrainingClusters.empty())
         throw std::runtime_error(
            "RClusterLoader::SplitDataset: no entries for training after split. "
            "Reduce validation_split.");
   
      if (fValidationSplit > 0.0f && fValidationClusters.empty())
         throw std::runtime_error(
            "RClusterLoader::SplitDataset: no entries for validation after split. "
            "Increase validation_split.");
            
      SortAndMergeClusters(fTrainingClusters, maxSize);
      SortAndMergeClusters(fValidationClusters, maxSize);

      for (const auto &c : fTrainingClusters)  fNumTrainingEntries  += c.numEntries();
      for (const auto &c : fValidationClusters) fNumValidationEntries += c.numEntries();
   }

   // sort the cluster indices after shuffling and splitting, to allow contiguous reads when possible
   void SortAndMergeClusters(std::vector<RClusterRange> &clusters, std::size_t maxSize)
   {
      if (clusters.empty())
         return;

      // Sort by file position so adjacent clusters can be merged
      std::sort(clusters.begin(), clusters.end(), [](const RClusterRange &a, const RClusterRange &b) {
         return a.rdfIdx < b.rdfIdx || (a.rdfIdx == b.rdfIdx && a.start < b.start);
      });

      // Merge clusters that are contiguous in the same file
      std::vector<RClusterRange> merged;
      merged.push_back(clusters[0]);
      for (std::size_t i = 1; i < clusters.size(); ++i) {
         const auto &cur  = clusters[i];
         auto &back = merged.back();
         const bool contiguous = (cur.rdfIdx == back.rdfIdx && cur.start == back.end);
         const bool withinLimit = (maxSize == 0 || back.numEntries() + cur.numEntries() <= maxSize);
         if (contiguous && withinLimit) {
            back.end = cur.end; // extend
         } else {
            merged.push_back(cur);
         }
      }
      clusters = std::move(merged);
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief store [start, end) pairs for each buffer fill, shuffled
   void ShuffleTrainingClusters(std::size_t highWatermark)
   {
      fTrainingClusterRanges.clear();
   
      std::size_t pos = 0;
      while (pos < fTrainingClusters.size()) {
         std::size_t fillStart    = pos;
         std::size_t accumulatedBufferSize  = 0;
         while (pos < fTrainingClusters.size() &&  (accumulatedBufferSize == 0 || accumulatedBufferSize + fTrainingClusters[pos].numEntries() <= highWatermark)) {
            accumulatedBufferSize += fTrainingClusters[pos].numEntries();
            ++pos;
         }
         fTrainingClusterRanges.emplace_back(fillStart, pos);
      }
   
      if (fShuffle) {
         std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
         std::shuffle(fTrainingClusterRanges.begin(), fTrainingClusterRanges.end(), g);
      }

      // printfTrainingClusterRanges for debugging
      // std::cout << "Training cluster ranges (size: " << fTrainingClusterRanges.size() << "):\n";
      // for (const auto &range : fTrainingClusterRanges) {
      //    std::size_t startIdx = range.first;
      //    std::size_t endIdx = range.second;
      //    std::size_t rangeSize = 0;
      //    for (std::size_t i = startIdx; i < endIdx; ++i) {
      //       rangeSize += fTrainingClusters[i].numEntries();
      //    }
      //    std::cout << "  [" << startIdx << ", " << endIdx << ")  entries: " << rangeSize << "\n";
      // }
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Shuffle the validation cluster order for the upcoming epoch.
   void ShuffleValidationClusters(std::size_t highWatermark)
   {
      fValidationClusterRanges.clear();
   
      std::size_t pos = 0;
      while (pos < fValidationClusters.size()) {
         std::size_t fillStart    = pos;
         std::size_t accumulatedBufferSize  = 0;
         while (pos < fValidationClusters.size() && accumulatedBufferSize < highWatermark) {
            accumulatedBufferSize += fValidationClusters[pos].numEntries();
            ++pos;
         }
         fValidationClusterRanges.emplace_back(fillStart, pos);
      }
   
      if (fShuffle) {
         std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
         std::shuffle(fValidationClusterRanges.begin(), fValidationClusterRanges.end(), g);
      }
   }

   void LoadClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx, const ULong64_t &startRow, const ULong64_t &endRow)
   {
      ROOT::RDF::RNode &rdf = fRdfs[rdfIdx];
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, startRow, endRow);
      RChunkLoaderFunctor<Args...> func(dest, fNumChunkCols, fVecSizes, fVecPadding, 0);
      rdf.Foreach(func, fCols);
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, 0, fTotalEntries);
   }

   void LoadTrainingClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx, ULong64_t startRow, ULong64_t endRow)
   {
      LoadClusterInto(dest, rdfIdx, startRow, endRow);
   }

   void LoadValidationClusterInto(RFlat2DMatrix &dest, std::size_t rdfIdx, ULong64_t startRow, ULong64_t endRow)
   {
      LoadClusterInto(dest, rdfIdx, startRow, endRow);
   }

   //////////////////////////////////////////////////////////////////////////
   // Accessors
   std::size_t GetNumTrainingEntries()   const { return fNumTrainingEntries; }
   std::size_t GetNumValidationEntries() const { return fNumValidationEntries; }
   std::size_t GetNumChunkCols() const { return fNumChunkCols; }

   const std::vector<RClusterRange>& GetTrainingClusters() const { return fTrainingClusters; }
   const std::vector<RClusterRange>& GetValidationClusters() const { return fValidationClusters; }
   
   const std::vector<std::pair<std::size_t, std::size_t>>& GetTrainingClusterRanges() const { return fTrainingClusterRanges; }
   const std::vector<std::pair<std::size_t, std::size_t>>& GetValidationClusterRanges() const { return fValidationClusterRanges; }

   const std::size_t GetNumTrainingClusterRanges() const { return fTrainingClusterRanges.size(); }
   const std::size_t GetNumValidationClusterRanges() const { return fValidationClusterRanges.size(); }

   // DBG
   void PrintClusterInfo(const std::string &label = "") const
   {
      if (!label.empty())
         std::cout << "\n=== " << label << " ===\n";

      std::cout << "Total clusters : " << fAllClusters.size()
               << "  (entries: ";
      std::size_t total = 0;
      for (const auto &c : fAllClusters) total += c.end - c.start;
      std::cout << total << ")\n";

      std::cout << "Training clusters  : " << fTrainingClusters.size()
               << "  (entries: " << fNumTrainingEntries << ")\n";
      for (std::size_t i = 0; i < fTrainingClusters.size(); ++i) {
         const auto &c = fTrainingClusters[i];
         std::cout << "  [" << i << "] rdf=" << c.rdfIdx
                  << "  entries=[" << c.start << ", " << c.end << ")"
                  << "  size=" << (c.end - c.start) << "\n";
      }

      std::cout << "Validation clusters: " << fValidationClusters.size()
               << "  (entries: " << fNumValidationEntries << ")\n";
      for (std::size_t i = 0; i < fValidationClusters.size(); ++i) {
         const auto &c = fValidationClusters[i];
         std::cout << "  [" << i << "] rdf=" << c.rdfIdx
                  << "  entries=[" << c.start << ", " << c.end << ")"
                  << "  size=" << (c.end - c.start) << "\n";
      }
      std::cout << std::flush;
   }
};

} // namespace ROOT::Experimental::Internal::ML
#endif // ROOT_INTERNAL_ML_RCHUNKLOADER
