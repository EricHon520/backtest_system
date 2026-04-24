#include <pybind11/pybind11.h>
#include <cmath>
#include <string>  
#include <algorithm>

double normalize_price(double price, double tick_size) {
    return std::round(price / tick_size) * tick_size;
}

double calculate_commission(
    const std::string& symbol, int quantity, double price, 
    const std::string& direction, int contract_multiplier, double commission_rate, 
    double min_commission, bool stamp_duty_on_sell_only, double stamp_duty, 
    double transfer_fee, double trading_fee
) {
    double trade_value = quantity * price * contract_multiplier;

    // Commission
    double commission = std::max(trade_value * commission_rate, min_commission);

    // Stamp Duty
    double cal_stamp_duty;
    if (stamp_duty_on_sell_only) {
        cal_stamp_duty = (direction == "SELL")? trade_value * stamp_duty : 0.0;
    } 
    else {
        cal_stamp_duty = trade_value * stamp_duty;
    }
    
    // Transfer Fee
    double cal_transfer_fee = trade_value * transfer_fee;

    // Trading Fee
    double cal_trading_fee = trade_value * trading_fee;
    
    return commission + cal_stamp_duty + cal_transfer_fee + cal_trading_fee;
}

double calculate_slippage(
    const std::string& symbol, int quantity, double price,
    const std::string& direction, double bar_volume, double bar_high, double bar_low,
    const std::string& slippage_model, double slippage_bps, double volume_slippage_factor
) {
    if (slippage_model == "none") {
        return price;
    }

    if (slippage_model == "fixed") {
        double slippage_pct = slippage_bps / 10000.0;
        if (direction == "BUY") {
            return price * (1.0 + slippage_pct);
        }
        else {
            return price * (1.0 - slippage_pct);
        }
    }
    else if (slippage_model == "volume_based") {
        double slippage_pct;
        if (bar_volume <= 0) {
            slippage_pct = 0.001;
        }
        else {
            double order_volume_pct = quantity / bar_volume;
            double spread_pct = (bar_high - bar_low) / price;
            slippage_pct = volume_slippage_factor * std::sqrt(order_volume_pct) * spread_pct;
            slippage_pct = std::min(slippage_pct, 0.01);
        }

        if (direction == "BUY") {
            return price * (1.0 + slippage_pct);
        }
        else {
            return price * (1.0 - slippage_pct);
        }   
    }
    else if (slippage_model == "spread_based") {
        double spread_pct = (bar_high - bar_low) / price;
        double slippage_pct = spread_pct * 0.5;
        if (direction == "BUY") {
            return price * (1.0 + slippage_pct);
        }
        else {
            return price * (1.0 - slippage_pct);
        }
    }
    return price;
}

namespace py = pybind11;

PYBIND11_MODULE(market_rule_ext, m) {
    m.doc() = "Market rule calculations in C++";

    m.def("normalize_price", &normalize_price, "Normalize price to tick size");
    m.def("calculate_commission", &calculate_commission, "Calculate commission and fees");
    m.def("calculate_slippage", &calculate_slippage, "Calculate slippage-adj price");
}