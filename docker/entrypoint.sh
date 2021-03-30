#!/bin/bash --login
set -e

echo $ROOT_PASSWD | sudo -S service ssh start

cp -r /$GENE_FILES $HOME/$GENE_FILES

conda activate $HOME/app/env
pip3 install -r $HOME/app/requirements.txt

exec "$@"