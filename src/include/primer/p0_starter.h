//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c) { this->linear = new T[r * c]{}; }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] this->linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    T *addr = this->linear;
    this->data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      this->data_[i] = addr;
      addr += c;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return this->data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { this->data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    int r = this->GetRows();
    int c = this->GetColumns();
    for (int i = 0, sz = r * c; i < sz; i++) {
      this->linear[i] = arr[i];
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] this->data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    if (!mat1 || !mat2) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int rows = mat1->GetRows();
    int cols = mat1->GetColumns();
    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
    for (int row = 0; row < rows; row++) {
      for (int col = 0; col < cols; col++) {
        T sum = mat1->GetElem(row, col) + mat2->GetElem(row, col);
        ret->SetElem(row, col, sum);
      }
    }
    return ret;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    if (!mat1 || !mat2) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int mat1_rows = mat1->GetRows();
    int mat1_cols = mat1->GetColumns();
    int mat2_cols = mat2->GetColumns();
    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(mat1_rows, mat2_cols));
    /* for an matrix multiplication, c[i][j] = SUM(a[i][k] * b[k][j]),
     * where k is in range [0, mat1_cols), therefore
     * c[i][j] += a[i][k] * b[k][j]
     */
    for (int i = 0; i < mat1_rows; i++) {
      for (int k = 0; k < mat1_cols; k++) {
        for (int j = 0; j < mat2_cols; j++) {
          T val = ret->GetElem(i, j);
          val += mat1->GetElem(i, k) * mat2->GetElem(k, j);
          ret->SetElem(i, j, val);
        }
      }
    }
    return ret;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    std::unique_ptr<RowMatrix<T>> temp(MultiplyMatrices(move(matA), move(matB)).release());
    if (!temp) {
      return std::unique_ptr<RowMatrix<T>>(temp.release());
    }
    return std::unique_ptr<RowMatrix<T>>(AddMatrices(move(temp), move(matC)).release());
  }
};
}  // namespace bustub
