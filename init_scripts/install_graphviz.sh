#! /usr/bin/env bash

echo "$DB_IS_DRIVER"
if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  echo 'Driver Installs...'
  apt-get install -y graphviz
  pip install graphviz
else
  echo 'Worker Installs...'
fi
echo 'Driver & Worker Installs...'
