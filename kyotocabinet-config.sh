#!/bin/sh

git submodule init >kyotocabinet_node_configure.out 2>&1
git submodule update >>kyotocabinet_node_configure.out 2>&1
cd deps/kyotocabinet
make clean >>../../kyotocabinet_node_configure.out 2>&1
sh configure --disable-shared --enable-static >>../../kyotocabinet_node_configure.out 2>&1
cd ../..
