# Many linux dockerfile with Rust, Node, and Lance dependencies installed.
# This container allows building the node modules native libraries in an 
# environment with a very old glibc, so that we are compatible with a wide
# range of linux distributions.
ARG ARCH=x86_64

FROM quay.io/pypa/manylinux2014_${ARCH}

ARG ARCH=x86_64
ARG DOCKER_USER=default_user

# System dependencies are installed as root
RUN yum install -y unzip

# Install static openssl
COPY install_openssl.sh install_openssl.sh
RUN bash ./install_openssl.sh ${ARCH}

# Protobuf is also installed as root.
COPY install_protobuf.sh install_protobuf.sh
RUN bash ./install_protobuf.sh ${ARCH}

ENV DOCKER_USER=${DOCKER_USER}
# Create a group and user
RUN echo ${ARCH} && adduser --user-group --create-home ${DOCKER_USER} && \
    usermod -aG wheel ${DOCKER_USER}

# We switch to the user to install Rust and Node, since those like to be 
# installed at the user level.
USER ${DOCKER_USER}

COPY prepare_manylinux_node.sh prepare_manylinux_node.sh
# COPY nvm.sh /etc/profile.d/nvm.sh
RUN cp /prepare_manylinux_node.sh $HOME/ && \
    cd $HOME && \
    bash ./prepare_manylinux_node.sh ${ARCH}

ENV OPENSSL_STATIC=1
ENV OPENSSL_LIB_DIR=/usr/local/lib64/
ENV OPENSSL_INCLUDE_DIR=/usr/local/include/openssl
