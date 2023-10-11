counter = 0

request = function()
    wrk.method = "GET"
    counter = (counter + 1)
    counter_str = tostring(counter)
    key = "key" .. counter_str
    path = "/key/" .. key
    --io.write(string.format("probe %s %d\n", key, counter_str))
    return wrk.format(nil, path)
end

done = function(summary, latency, requests)
    io.write(string.format("total requests: %d\n", summary.requests))
end
