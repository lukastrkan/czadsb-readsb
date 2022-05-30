#!/bin/bash

cd eredis
git submodule update --init
cmake .
make
sudo make install
cd ..
make
