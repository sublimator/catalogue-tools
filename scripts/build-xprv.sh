#!/bin/bash
cd build && ninja xprv 2>&1 | tee /tmp/catalogue_tools_build.txt
