#!/bin/bash
cd build && ninja xprv 2>&1 | tail -5
