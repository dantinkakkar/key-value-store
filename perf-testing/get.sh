wrk -t1 -c1 -d30s -s scripts/get.lua http://localhost:8000/ --latency
