// Author: Silia Taider, CERN 08/2026

/*************************************************************************
 * Copyright (C) 1995-2026, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_INTERNAL_ML_RBATCHSINK
#define ROOT_INTERNAL_ML_RBATCHSINK

#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "ROOT/ML/RFlat2DMatrix.hxx"
#include "ROOT/RNTupleModel.hxx"
#include "ROOT/RNTupleWriter.hxx"
#include "ROOT/RSnapshotOptions.hxx" // only for ESnapshotOutputFormat, the same enum Snapshot() uses
#include "TFile.h"
#include "TTree.h"

namespace ROOT::Experimental::Internal::ML {

/**
 * \class RBatchSink
 * \brief Consumes RFlat2DMatrix batches and writes their rows to disk under their *original*
 * column names -- a vector column that was expanded into N tensor columns is written back as
 * one fixed-size, padded, float vector column, not N scalars, so it can still be named as a
 * single column if this file is loaded again. Lets RDataLoaderEngine::Save() stay agnostic to
 * the output format ("sink" as in source/sink: the write-side end of a data pipeline, same
 * naming RNTuple itself uses internally for RPageSink/RPageSource).
 *
 * \param names Original (pre-expansion) column names, RDataLoader.given_columns.
 * \param widths Width in tensor columns of each entry in \p names: 1 for a scalar, the vector's
 * max size otherwise. Parallel to \p names, RDataLoader.column_widths.
 */
class RBatchSink {
public:
   virtual ~RBatchSink() = default;
   virtual void FillBatch(const RFlat2DMatrix &batch) = 0;
};

/// \brief Writes batches into a flat TTree: one Float_t branch per scalar column, one
/// std::vector<float> branch per (originally-vector) column.
class RTTreeBatchSink final : public RBatchSink {
   std::unique_ptr<TFile> fFile;
   TTree *fTree; // owned by fFile
   std::vector<std::size_t> fWidths;
   std::vector<float> fScalarRow;               // one slot per scalar column
   std::vector<std::vector<float>> fVectorRow;   // one slot per vector column

public:
   RTTreeBatchSink(std::string_view treename, std::string_view filename, const std::vector<std::string> &names,
                   const std::vector<std::size_t> &widths)
      : fFile(TFile::Open(std::string(filename).c_str(), "RECREATE")), fWidths(widths)
   {
      fTree = new TTree(std::string(treename).c_str(), std::string(treename).c_str());
      fTree->SetDirectory(fFile.get());

      // Branch() binds to these addresses; size both row buffers up front so a later
      // resize/emplace can't reallocate and leave a branch pointing at freed memory.
      const std::size_t numScalar = std::count(widths.begin(), widths.end(), 1);
      fScalarRow.resize(numScalar);
      fVectorRow.resize(widths.size() - numScalar);

      std::size_t si = 0, vi = 0;
      for (std::size_t i = 0; i < names.size(); i++) {
         if (widths[i] == 1) {
            fTree->Branch(names[i].c_str(), &fScalarRow[si++]);
         } else {
            fVectorRow[vi].resize(widths[i]);
            fTree->Branch(names[i].c_str(), &fVectorRow[vi]);
            vi++;
         }
      }
   }

   void FillBatch(const RFlat2DMatrix &batch) override
   {
      const std::size_t cols = batch.GetCols();
      const float *data = batch.GetData();
      for (std::size_t r = 0; r < batch.GetRows(); r++) {
         const float *row = data + r * cols;
         std::size_t offset = 0, si = 0, vi = 0;
         for (std::size_t w : fWidths) {
            if (w == 1)
               fScalarRow[si++] = row[offset];
            else
               std::copy(row + offset, row + offset + w, fVectorRow[vi++].begin());
            offset += w;
         }
         fTree->Fill();
      }
   }

   ~RTTreeBatchSink() override { fFile->Write(); }
};

/// \brief Writes batches into an RNTuple: one float field per scalar column, one
/// std::vector<float> field per (originally-vector) column.
class RNTupleBatchSink final : public RBatchSink {
   std::vector<std::size_t> fWidths;
   std::vector<std::shared_ptr<float>> fScalarFields;
   std::vector<std::shared_ptr<std::vector<float>>> fVectorFields;
   std::unique_ptr<ROOT::RNTupleWriter> fWriter;

public:
   RNTupleBatchSink(std::string_view treename, std::string_view filename, const std::vector<std::string> &names,
                     const std::vector<std::size_t> &widths)
      : fWidths(widths)
   {
      auto model = ROOT::RNTupleModel::Create();
      for (std::size_t i = 0; i < names.size(); i++) {
         if (widths[i] == 1) {
            fScalarFields.push_back(model->MakeField<float>(names[i]));
         } else {
            auto field = model->MakeField<std::vector<float>>(names[i]);
            field->resize(widths[i]);
            fVectorFields.push_back(field);
         }
      }
      fWriter = ROOT::RNTupleWriter::Recreate(std::move(model), std::string(treename), std::string(filename));
   }

   void FillBatch(const RFlat2DMatrix &batch) override
   {
      const std::size_t cols = batch.GetCols();
      const float *data = batch.GetData();
      for (std::size_t r = 0; r < batch.GetRows(); r++) {
         const float *row = data + r * cols;
         std::size_t offset = 0, si = 0, vi = 0;
         for (std::size_t w : fWidths) {
            if (w == 1)
               *fScalarFields[si++] = row[offset];
            else
               std::copy(row + offset, row + offset + w, fVectorFields[vi++]->begin());
            offset += w;
         }
         fWriter->Fill();
      }
   }
};

/// \brief Picks the sink implementation from the same output-format enum Snapshot() uses.
inline std::unique_ptr<RBatchSink> CreateBatchSink(std::string_view treename, std::string_view filename,
                                                    const std::vector<std::string> &names,
                                                    const std::vector<std::size_t> &widths,
                                                    ROOT::RDF::ESnapshotOutputFormat format)
{
   if (format == ROOT::RDF::ESnapshotOutputFormat::kRNTuple)
      return std::make_unique<RNTupleBatchSink>(treename, filename, names, widths);
   return std::make_unique<RTTreeBatchSink>(treename, filename, names, widths);
}

} // namespace ROOT::Experimental::Internal::ML
#endif // ROOT_INTERNAL_ML_RBATCHSINK
