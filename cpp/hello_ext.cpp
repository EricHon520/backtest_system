// hello_ext.cpp — 第一個 C++ 擴展，用來驗證環境是否正確
//
// C++ 基礎語法說明：
//   #include  = 相當於 Python 的 import
//   //        = 單行注釋（像 Python 的 #）
//   /* ... */ = 多行注釋

#include <pybind11/pybind11.h>   // pybind11 核心標頭檔
#include <string>                 // std::string（C++ 字串型別）

namespace py = pybind11;          // 命名空間別名，之後可以寫 py:: 代替 pybind11::


// C++ 函數定義：
//   回傳型別  函數名稱(參數型別 參數名稱)
//   ↓         ↓      ↓
//   std::string  greet(const std::string& name)
//
//   const std::string& = 傳入一個「不可修改的字串引用」（比複製整個字串更快）
std::string greet(const std::string& name) {
    return "Hello from C++, " + name + "!";
    //      字串拼接用 +，和 Python 一樣
}


// C++ 加法函數（展示基本數值型別）
//   double = 64-bit 浮點數（等同 Python 的 float）
//   int    = 32-bit 整數（等同 Python 的 int，但有大小限制）
double add(double a, double b) {
    return a + b;
}


// PYBIND11_MODULE 是 pybind11 提供的巨集（macro）
// 它告訴 Python：「這個 .so 檔案裡有一個叫 hello_ext 的模組」
//
//   第一個參數 hello_ext = 模組名稱，必須和檔案名一致
//   第二個參數 m        = 模組物件，用來註冊函數
PYBIND11_MODULE(hello_ext, m) {
    m.doc() = "Hello World C++ extension for learning pybind11";

    // m.def("Python裡的函數名", C++函數指標, "說明文字")
    m.def("greet", &greet, "Return a greeting string");
    m.def("add",   &add,   "Add two numbers");
}
