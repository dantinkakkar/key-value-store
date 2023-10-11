counter = 0

request = function()
    wrk.method = "PUT"
    wrk.headers["Content-Type"] = "application/json"
    counter = (counter + 1)
    counter_str = tostring(counter)
    key = "key-" .. counter_str
    value = "value-" .. counter_str
    -- change this to your path for Set e.g.
    path = "/key/" .. key .. "/value/" .. value
    body = value
    --io.write(string.format("key %s %d\n", key, counter_str))
    --io.write(body)
    return wrk.format(nil, path, nil, body)
end

done = function(summary, latency, requests)
    io.write(string.format("total requests: %d\n", summary.requests))
end
