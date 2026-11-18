FROM elixir:1.19.5-otp-28
RUN apt-get update && apt-get install -y --no-install-recommends \
      iproute2 nftables tcpdump gcc libc6-dev curl ca-certificates rsync util-linux procps kmod iputils-ping time \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.94.0
ENV PATH=/root/.cargo/bin:$PATH
RUN mix local.hex --force && mix local.rebar --force
