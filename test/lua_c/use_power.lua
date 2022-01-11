function call_go(r, a_number)
    local power = require("power")
    local r = power.go(a_number);
    info(r)
    return r
end
