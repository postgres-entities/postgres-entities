#!/bin/bash
for i in 0 1 2 3 4 5 ; do
  node perf_runner.js &
done
wait
