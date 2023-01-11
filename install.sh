#!/bin/bash

pip uninstall fennel-client-lib
pip uninstall fennel-dataset

# List and install whl files in gen_rust_lib
search_dir=fennel/test_lib/gen_rust_lib
for entry in "$search_dir"/*
do
  echo "Installing $entry"
  pip install $entry --force-reinstall
done