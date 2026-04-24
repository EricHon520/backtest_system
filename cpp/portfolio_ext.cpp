#include "pybind11/cast.h"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <string>
#include <cstddef>

namespace py = pybind11;

py::tuple compute_mtm(
    const std::vector<std::string>& symbols,
    const std::vector<double>& avg_costs,
    const std::vector<int>& quantities,
    const std::vector<double>& current_prices,
    const std::vector<int>& contract_multipliers,
    const std::vector<double>& last_settle_prices,
    const std::vector<bool>& is_daily_settlement

) {
    double total_market_value = 0.0;

    std::vector<double> unrealized_pnl;
    unrealized_pnl.reserve(symbols.size());

    for (size_t i = 0; i < symbols.size(); ++i) {
        double unrealized;

        if (is_daily_settlement[i]) {
            double prev_price = last_settle_prices[i];
            if (prev_price == 0.0) {
                prev_price = avg_costs[i];
            }
            unrealized = (current_prices[i] - prev_price) * quantities[i] * contract_multipliers[i];
        }
        else {
            unrealized = (current_prices[i] - avg_costs[i]) * quantities[i] * contract_multipliers[i];
        }        

        unrealized_pnl.push_back(unrealized);
        total_market_value += current_prices[i] * quantities[i] * contract_multipliers[i];
    }

    return py::make_tuple(unrealized_pnl, total_market_value);
}

PYBIND11_MODULE(portfolio_ext, m) {
    m.doc() = "Portfolio calculations in C++";
    m.def("compute_mtm", &compute_mtm);
}