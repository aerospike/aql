
local function bin(name)
    local function x(rec)
        return rec[name]
    end
    return x
end

local function even(a)
    return a % 2 == 0
end

local function add(a,b)
    return a + b
end

local function multiplier(factor)
    local function x(a)
        return a * factor
    end
    return x
end

function foo(rec)
    return bin("a")(rec)
end

function a(stream)
    return stream : map(bin("a"))
end

function even_a(stream)
    return stream : map(bin("a")) : filter(even)
end

function sum_even_a(stream)
    return stream : map(bin("a")) : filter(even) : reduce(add)
end

function sum_even_a_x(stream, factor)
    return stream : map(bin("a")) : filter(even) : reduce(add) : map(multiplier(factor))
end