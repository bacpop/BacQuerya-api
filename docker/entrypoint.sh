#!/bin/bash --login
set -e

echo $ROOT_PASSWD | sudo -S service ssh start

cp -r /$GENE_FILES $HOME/$GENE_FILES

conda activate $HOME/app/env

# install latest COBS
RUN git clone --recursive https://github.com/bingmann/cobs.git && cd cobs && python setup.py install && cd ..

exec "$@"