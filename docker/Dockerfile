# From https://github.com/kaust-vislab/python-data-science-project
FROM ubuntu:20.04

LABEL maintainer="danielanderson <da916@ic.ac.uk>"

SHELL [ "/bin/bash", "--login", "-c" ]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 curl git cmake g++ zlib1g-dev gnupg vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install ssh
ENV SSH_PASSWD "root:Docker!"
ENV ROOT_PASSWD "Docker!"
RUN apt-get update \
  && apt-get install -y --no-install-recommends dialog \
  && apt-get update \
  && apt-get install -y --no-install-recommends openssh-server \
  && apt-get update \
  && apt-get install -y --no-install-recommends sudo \
  && echo "$SSH_PASSWD" | chpasswd
COPY docker/sshd_config /etc/ssh
# Use root password for sudo access
RUN echo "Defaults rootpw" >> /etc/sudoers

#install ODBC driver for azure sql database access
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN exit
RUN sudo apt-get update
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN sudo ACCEPT_EULA=Y apt-get install -y mssql-tools

# Create a non-root user
ARG username=bacquerya-usr
ARG uid=1000
ARG gid=100
ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER

RUN adduser --disabled-password \
    --gecos "Non-root user" \
    --uid $UID \
    --gid $GID \
    --home $HOME \
    $USER
RUN usermod -aG sudo $USER

COPY environment.yml /tmp/
RUN chown $UID:$GID /tmp/environment.yml

COPY docker/entrypoint.sh /usr/local/bin/
RUN chown $UID:$GID /usr/local/bin/entrypoint.sh && \
    chmod u+x /usr/local/bin/entrypoint.sh

USER $USER

# install miniconda
ENV MINICONDA_VERSION latest
ENV CONDA_DIR $HOME/miniconda3
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-$MINICONDA_VERSION-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh && \
    ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh

# make non-activate conda commands available
ENV PATH=$CONDA_DIR/bin:$PATH

# make conda activate command available from /bin/bash --login shells
RUN echo ". $CONDA_DIR/etc/profile.d/conda.sh" >> ~/.profile

# make conda activate command available from /bin/bash --interative shells
RUN conda init bash

# create a project directory inside user home
ENV PROJECT_DIR $HOME/app
RUN mkdir $PROJECT_DIR
# copy the code in
COPY . $PROJECT_DIR
WORKDIR $PROJECT_DIR

# build the conda environment
ENV ENV_PREFIX $PROJECT_DIR/env
RUN conda update --name base --channel defaults conda && \
    conda env create --prefix $ENV_PREFIX --file /tmp/environment.yml --force && \
    conda clean --all --yes

# use an entrypoint script to insure conda environment is properly activated at runtime
USER root
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]

# default command will be to launch flask server for deployment
# see https://pythonspeed.com/articles/gunicorn-in-docker/
EXPOSE 8000 2222
CMD [ "gunicorn", \
      "-b", "0.0.0.0:8000", \
      "--worker-tmp-dir", "/dev/shm", \
      "--log-file=-", \
      "--timeout", "600", \
      "--workers=4", "--threads=2", "--worker-class=gthread", \
      "--chdir", "BacQuerya_api", \
      "api:app" ]