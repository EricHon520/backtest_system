#include <pybind11/pybind11.h>
#include <deque>

namespace py = pybind11;

class RollingWindow {
    protected:
        std::deque<double> window; // store historical price
        int max_size; // max window size
        double running_sum; // running sum of the window

    public:
        RollingWindow(int size): max_size(size), running_sum(0.0) {}

        // update the window with a new value, remove the oldest value if window is full
        virtual void update(double value) {
            if (window.size() >= max_size) {
                running_sum -= window.front();
                window.pop_front();
            }
            window.push_back(value);
            running_sum += value;
        }

        double average() const {
            if (window.empty()) {
                return 0.0;
            }
            return running_sum / window.size();
        }

        int size() const {
            return window.size();
        }

};

class RollingMA: public RollingWindow {
    public: 
        RollingMA(int window_size): RollingWindow(window_size) {}
    
    // calculate the moving average
    double calculate() const {
        return average();
    }
};

class RollingEMA: public RollingWindow {
    private:
        double ema_value;
        double multiplier;
    
    public:
        RollingEMA(int window_size): RollingWindow(window_size), ema_value(0.0) {
            multiplier = 2.0 / (window_size + 1);
        }

        void update(double value) override {
            if (ema_value == 0.0) {
                ema_value = value;
            }
            else {
                ema_value = value * multiplier + ema_value * (1 - multiplier);
            }
            RollingWindow::update(ema_value);
        }

        double calculate() const {
            return ema_value;
        }
};

namespace py = pybind11;

PYBIND11_MODULE(indicators_ext, m) {
    m.doc()= "Rolling indicators in C++";

    py::class_<RollingWindow>(m, "RollingWindow")
        .def(py::init<int>())
        .def("update", &RollingWindow::update)
        .def("average", &RollingWindow::average)
        .def("size", &RollingWindow::size);

    py::class_<RollingMA, RollingWindow>(m, "RollingMA")
        .def(py::init<int>())
        .def("calculate", &RollingMA::calculate);

    py::class_<RollingEMA, RollingWindow>(m, "RollingEMA")
        .def(py::init<int>())
        .def("update", &RollingEMA::update)
        .def("calculate", &RollingEMA::calculate);
}
