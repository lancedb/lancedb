# Many linux dockerfile with Rust, Node, and Lance dependencies installed
ARG ARCH=x64

FROM quay.io/pypa/manylinux2014_${ARCH}

ENV ARCH=${ARCH}

COPY prepare_manylinux_node.sh /home/prepare_manylinux_node.sh
RUN  bash /home/prepare_manylinux_node.sh ${ARCH}

ENV OPENSSL_STATIC=1
ENV OPENSSL_LIB_DIR=/usr/lib64/
ENV OPENSSL_INCLUDE_DIR=/usr/include/openssl
