#ifndef ROOT_INTERNAL_ML_RFLAT2DMATRIX
#define ROOT_INTERNAL_ML_RFLAT2DMATRIX

#include <cassert>
#include <utility>
#include <iostream>

#include "ROOT/RVec.hxx"

namespace ROOT::Experimental::Internal::ML {
/// \brief Wrapper around ROOT::RVec<float> representing a 2D matrix
///
/// The storage is flattened row-major: index(row, col) == row * cols + col.
struct RFlat2DMatrix {
   ROOT::RVecF fRVec;
   std::size_t fRows{0};
   std::size_t fCols{0};

   RFlat2DMatrix() = default;

   RFlat2DMatrix(std::size_t rows, std::size_t cols) { Resize(rows, cols); }

   float *GetData() { return fRVec.data(); }

   const float *GetData() const { return fRVec.data(); }

   // Used in the pythonization
   std::pair<std::size_t, std::size_t> GetShape() const { return {fRows, fCols}; }

   std::size_t GetRows() const { return fRows; }

   std::size_t GetCols() const { return fCols; }

   std::size_t GetSize() const { return fRVec.size(); }

   void Resize(std::size_t rows, std::size_t cols)
   {
      fRows = rows;
      fCols = cols;
      fRVec.resize(rows * cols);
   }

   void Reshape(std::size_t rows, std::size_t cols)
   {
      // We don't reallocate: require matching sizes
      assert(rows * cols == fRVec.size());
      fRows = rows;
      fCols = cols;
   }

   /// \brief Append all rows from another matrix to this one.
   /// Both matrices must have the same number of columns.
   void Append(const RFlat2DMatrix &other)
   {
      if (other.GetRows() == 0) return;

      // if buffer is empty, inherit column count from the first cluster
      if (fRows == 0 && fCols == 0) {
         fCols = other.GetCols();
      }

      assert(fCols == other.GetCols() && "Append: column count mismatch");

      const std::size_t oldRows = fRows;
      const std::size_t newRows = oldRows + other.GetRows();
      Resize(newRows, fCols);
      std::copy(other.GetData(),
               other.GetData() + other.GetRows() * other.GetCols(),
               GetData() + oldRows * fCols);
   }

   std::size_t ExtendRows(std::size_t extraRows, std::size_t cols)
   {
      if (fRows == 0 && fCols == 0)
         fCols = cols;
      assert(fCols == cols && "ExtendRows: column count mismatch");
      const std::size_t startRow = fRows;
      Resize(fRows + extraRows, fCols);
      return startRow; // caller writes starting here
   }

   float &operator[](std::size_t i) { return fRVec[i]; }

   const float &operator[](std::size_t i) const { return fRVec[i]; }
};

} // namespace ROOT::Experimental::Internal::ML
#endif // ROOT_INTERNAL_ML_RFLAT2DMATRIX
