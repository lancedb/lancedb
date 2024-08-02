# Many linux dockerfile with Rust, Node, and Lance dependencies installed.
# This container allows building the node modules native libraries in an 
# environment with a very old glibc, so that we are compatible with a wide
# range of linux distributions.
ARG ARCH=x86_64

FROM quay.io/pypa/manylinux_2_28_${ARCH}

ARG ARCH=x86_64
ARG DOCKER_USER=default_user

# Install static openssl
COPY install_openssl.sh install_openssl.sh
RUN ./install_openssl.sh ${ARCH} > /dev/null

# Protobuf is also installed as root.
COPY install_protobuf.sh install_protobuf.sh
RUN ./install_protobuf.sh ${ARCH}

ENV DOCKER_USER=${DOCKER_USER}
# Create a group and user, but only if it doesn't exist
RUN echo ${ARCH} && id -u ${DOCKER_USER} >/dev/null 2>&1 || adduser --user-group --create-home --uid ${DOCKER_USER} build_user

# We switch to the user to install Rust and Node, since those like to be 
# installed at the user level.
USER ${DOCKER_USER}

COPY prepare_manylinux_node.sh prepare_manylinux_node.sh
RUN cp /prepare_manylinux_node.sh $HOME/ && \
    cd $HOME && \
    ./prepare_manylinux_node.sh ${ARCH}
