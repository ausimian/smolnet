#!/bin/sh
# Inside the container: sync the spike worktree, build in prod, run the
# unimpaired matrix with the NIF and then the spike engine, alternately.
set -eu
rsync -a --delete --exclude _build --exclude native/target --exclude priv/native \
  --exclude integration/runs --exclude integration/tun_helper/_build --exclude .git \
  /src/ /work/
cd /work
export MIX_ENV=prod
mix compile 2>&1 | grep -E "release mode|debug mode|error|Generated" || true
[ -f native/target/release/libsmolnet_nif.so ] || { echo "no release NIF"; exit 1; }
ls -la native/target/release/libsmolnet_nif.so
nproc
uname -r
cpus=${CPUS:-}
pin=""
[ -n "$cpus" ] && pin="taskset -c $cpus"
args="--no-pcap --profiles none --family inet --flows ${FLOWS:-send,receive,kernel} --streams 1,4 --bytes ${BYTES:-268435456} --repeats ${REPEATS:-1} --duration ${DURATION:-30m}"
rounds=${ROUNDS:-3}
i=1
while [ $i -le $rounds ]; do
  for engine in ${ENGINES:-nif spike spike-gc}; do
    out=/results/${TAG:-h2}-$engine-round$i
    rm -rf "$out"
    echo "== round $i engine $engine"
    case $engine in
      nif) env="SMOLNET_ENGINE=nif SPIKE_GC_TUNING=0" ;;
      spike) env="SMOLNET_ENGINE=spike SPIKE_GC_TUNING=0" ;;
      spike-gc) env="SMOLNET_ENGINE=spike SPIKE_GC_TUNING=1" ;;
    esac
    env $env /usr/bin/time -f "cpu user %U sys %S wall %e" -o "$out.time" $pin \
      integration/netem-topology.sh isolate \
      mix run integration/netem_matrix.exs $args --out "$out" > "$out.log" 2>&1 || echo "exit $? (see $out.log)"
    grep "\[spike\]" "$out.log" || true
    cat "$out.time"
    grep -E "Mbit/s|VERDICT" "$out.log" | tail -14
  done
  i=$((i + 1))
done
