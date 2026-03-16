// Author: Dante Niewenhuis, VU Amsterdam 07/2023
// Author: Kristupas Pranckietis, Vilnius University 05/2024
// Author: Nopphakorn Subsa-Ard, King Mongkut's University of Technology Thonburi (KMUTT) (TH) 08/2024
// Author: Vincenzo Eduardo Padulano, CERN 10/2024
// Author: Martin Føll, University of Oslo (UiO) & CERN 05/2025

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
\class ROOT::Experimental::Internal::ML::RChunkLoader

\brief Building and loading the chunks from the blocks and chunks constructed in RChunkConstructor

In this class the blocks are stiches together to form chunks that are loaded into memory. The blocks used to create each
chunk comes from different parts of the dataset. This is achieved by shuffling the blocks before distributing them into
chunks. The purpose of this process is to reduce bias during machine learning training by ensuring that the data is well
mixed. The dataset is also spit into training and validation sets with the user-defined validation split fraction.
*/

template <typename... Args>
class RChunkLoader {
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

   std::size_t fNumTrainingEntries{0};
   std::size_t fNumValidationEntries{0};

public:
   RChunkLoader(std::vector<ROOT::RDF::RNode> &rdfs,
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
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Distribute the blocks into training and validation datasets
   void SplitDataset()
   {
      const std::size_t numValidation = static_cast<std::size_t>(fValidationSplit * fAllClusters.size());
      const std::size_t numTraining = fAllClusters.size() - numValidation;

      // shuffle the cluster order before splitting into training and validation
      if (fShuffle) {
         std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
         std::shuffle(fAllClusters.begin(), fAllClusters.end(), g);
      }

      fTrainingClusters.assign(fAllClusters.begin(), fAllClusters.begin() + numTraining);
      fValidationClusters.assign(fAllClusters.begin() + numTraining, fAllClusters.end());

      for (const auto &c : fTrainingClusters) { fNumTrainingEntries += (c.end - c.start); }
      for (const auto &c : fValidationClusters) { fNumValidationEntries += (c.end - c.start); }
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Shuffle the training cluster order for the upcoming epoch.
   ///
   /// This is a pure index-level operation — no I/O. Called once per epoch
   /// by the loading thread before it starts consuming clusters.
   void ShuffleTrainingClusters()
   {
      if (!fShuffle) return;
      std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
      std::shuffle(fTrainingClusters.begin(), fTrainingClusters.end(), g);
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Shuffle the validation cluster order for the upcoming epoch.
   void ShuffleValidationClusters()
   {
      if (!fShuffle) return;
      std::mt19937 g(fSetSeed == 0 ? std::random_device{}() : fSetSeed);
      std::shuffle(fValidationClusters.begin(), fValidationClusters.end(), g);
   }

   //////////////////////////////////////////////////////////////////////////
   /// \brief Load one whole cluster into a flat matrix.
   ///
   /// Always reads an entire cluster — never splits across cluster boundaries.
   /// The output tensor is replaced (not appended to) by the cluster's rows.
   ///
   /// \param[out] tensor   Destination matrix, resized to (clusterSize, numCols).
   /// \param[in]  clusters Which cluster list to draw from (training or validation).
   /// \param[in]  idx      Index into that cluster list.
   void LoadCluster(RFlat2DMatrix &tensor, const std::vector<RClusterRange> &clusters, std::size_t idx)
   {
      const RClusterRange &c = clusters[idx];
      const std::size_t clusterSize = static_cast<std::size_t>(c.end - c.start);

      RFlat2DMatrix tmp(clusterSize, fNumChunkCols);

      ROOT::RDF::RNode &rdf = fRdfs[c.rdfIdx];
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, c.start, c.end);

      for (std::size_t row = 0; row < clusterSize; ++row) {
         RChunkLoaderFunctor<Args...> func(tmp, fNumChunkCols, fVecSizes, fVecPadding, row);
         rdf.Foreach(func, fCols);
      }

      // reset the dataframe range after loading
      ROOT::Internal::RDF::ChangeBeginAndEndEntries(rdf, 0, clusterSize);

      tensor = std::move(tmp);
   }

   // convenience wrappers used by the loading thread
   void LoadTrainingCluster(RFlat2DMatrix &tensor, std::size_t idx)
   {
      LoadCluster(tensor, fTrainingClusters, idx);
   }

   void LoadValidationCluster(RFlat2DMatrix &tensor, std::size_t idx)
   {
      LoadCluster(tensor, fValidationClusters, idx);
   }

   //////////////////////////////////////////////////////////////////////////
   // Accessors
   std::size_t GetNumTrainingEntries()   const { return fNumTrainingEntries; }
   std::size_t GetNumValidationEntries() const { return fNumValidationEntries; }
   std::size_t GetNumTrainingClusters()  const { return fTrainingClusters.size(); }
   std::size_t GetNumValidationClusters() const { return fValidationClusters.size(); }

   const std::vector<RClusterRange> &GetTrainingClusters()   const { return fTrainingClusters; }
   const std::vector<RClusterRange> &GetValidationClusters() const { return fValidationClusters; }


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
