#!/bin/bash

# List and install whl files in gen_rust_lib
search_dir=fennel/testing/gen_rust_lib
for entry in "$search_dir"/*
do
  echo "Installing $entry"
  pip3 install $entry --force-reinstall
done