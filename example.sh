#!/bin/sh

echo "In this setting, Basalt should succeed:"
cargo run -- -T 200 -n 1000 basalt -G -H -f 10 -t 300 -v 50 -i 50 -k 10 -r 10 

echo "With the same parameters, Brahms should fail:"
cargo run -- -T 200 -n 1000 brahms -G samples -f 10 -t 300 -v 50 -u 50 -k 10 -r 10 
