#!/bin/bash --login
set -e

echo $ROOT_PASSWD | sudo -S service ssh start

cp -r /$GENE_DICT_FILE $HOME/$GENE_DICT_FILE
cp -r /$GENE_INDEX $HOME/$GENE_INDEX

conda activate $HOME/app/env
exec "$@"