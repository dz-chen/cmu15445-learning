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

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r;
    cols = c;
    linear = new T[rows * cols];
  }

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
  // virtual ~Matrix() = default;
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    // 子类中调用父类的成员变量,这里需要加this指针编译才不报错...
    data_ = new T *[this->rows];
    for (int i = 0; i < this->rows; i++) {
      data_[i] = &(this->linear[i * this->cols]);
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  // 将一维数组按行主序转换为二维数组
  void MatImport(T *arr) override {
    for (int i = 0; i < this->rows; i++) {
      for (int j = 0; j < this->cols; j++) {
        data_[i][j] = arr[i * this->cols + j];
      }
    }
  }

  // TODO(P0): Add implementation
  // ~RowMatrix() override = default;
  ~RowMatrix() override { delete[] data_; }

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
    // TODO(P0): Add code
    int rows = mat1->GetRows();
    int cols = mat1->GetColumns();
    if (mat2->GetRows() != rows || mat2->GetColumns() != cols) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T elem1 = mat1->GetElem(i, j);
        T elem2 = mat2->GetElem(i, j);
        ret->SetElem(i, j, elem1 + elem2);
      }
    }
    return ret;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int rows = mat1->GetRows();
    int cols = mat2->GetColumns();
    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
    int num = mat1->GetColumns();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T tmp = T();  // 保证进行了初始化
        for (int k = 0; k < num; k++) {
          tmp += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        ret->SetElem(i, j, tmp);
      }
    }
    return ret;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    if (matA->GetColumns() != matB->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = matA->GetRows();
    int cols = matB->GetColumns();
    if (matC->GetRows() != rows || matC->GetColumns() != cols) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
    int num = matA->GetColumns();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T tmp = T();  // 保证进行了初始化
        for (int k = 0; k < num; k++) {
          tmp += matA->GetElem(i, k) * matB->GetElem(k, j);
        }
        ret->SetElem(i, j, tmp + matC->GetElem(i, j));
      }
    }
    return ret;
  }
};
}  // namespace bustub
