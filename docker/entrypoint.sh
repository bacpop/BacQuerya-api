#!/bin/bash --login
set -e

echo $ROOT_PASSWD | sudo -S service ssh start

cp -r /extracted_genes $HOME/extracted_genes
cp -r /index_genes $HOME/index_genes

conda activate $HOME/app/env
exec "$@"