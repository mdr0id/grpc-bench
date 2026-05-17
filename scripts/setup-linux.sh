#!/usr/bin/env bash
# grpc-bench: Ubuntu 24.04 host setup
#
# Installs the build toolchain, gRPC tooling, and applies the host
# tunables from RUNBOOK.md. Idempotent — safe to re-run.
#
# Usage:
#   sudo ./scripts/setup-linux.sh           # install + tune
#   sudo ./scripts/setup-linux.sh --build   # also `cargo build --release`
#                                           # and grant CAP_SYS_NICE
#
# Tested on Ubuntu 24.04 LTS (noble) on a DigitalOcean memory-optimized
# droplet. Should work unmodified on any Debian-derived 22.04+ / Ubuntu
# 24.04 host.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "must run as root (sudo $0 $*)" >&2
  exit 1
fi

DO_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --build) DO_BUILD=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

# The user who invoked sudo — used so cargo / rustup install under their
# home directory, not root's. Falls back to root if not running under
# sudo (rare; DO droplets run as root by default).
INVOKING_USER="${SUDO_USER:-root}"
INVOKING_HOME=$(eval echo "~${INVOKING_USER}")

echo "==> grpc-bench setup for ${INVOKING_USER} (${INVOKING_HOME})"

############################################################
# 1. APT packages — build toolchain, gRPC tools, host helpers
############################################################
echo "==> installing apt packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  build-essential \
  pkg-config \
  cmake \
  clang \
  libssl-dev \
  libclang-dev \
  protobuf-compiler \
  ca-certificates \
  curl \
  git \
  jq \
  libcap2-bin \
  chrony \
  linux-tools-common \
  "linux-tools-$(uname -r)" \
  unzip

systemctl enable --now chrony

############################################################
# 2. Rust toolchain via rustup (installs as the invoking user)
############################################################
if [[ ! -x "${INVOKING_HOME}/.cargo/bin/rustup" ]]; then
  echo "==> installing rustup under ${INVOKING_HOME}"
  sudo -u "${INVOKING_USER}" -H bash -c '
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain none --no-modify-path
  '
else
  echo "==> rustup already installed"
fi

# Add cargo bin to the user's shell rc if not already there. Skips on
# shells that don't source these files; the rustup installer also drops
# its own snippet.
for rc in "${INVOKING_HOME}/.bashrc" "${INVOKING_HOME}/.profile"; do
  if [[ -f "$rc" ]] && ! grep -q '.cargo/env' "$rc"; then
    echo '. "$HOME/.cargo/env"' >> "$rc"
  fi
done

# The repo's rust-toolchain.toml pins the channel; running rustup show
# in the repo will install whatever it specifies on first invocation.
# We don't install a toolchain here — let the repo decide on first build.

############################################################
# 3. gRPC test tooling — grpcurl
############################################################
GRPCURL_VERSION="1.9.1"
if ! command -v grpcurl >/dev/null 2>&1; then
  echo "==> installing grpcurl ${GRPCURL_VERSION}"
  arch=$(uname -m)
  case "$arch" in
    x86_64)  grpcurl_arch="x86_64" ;;
    aarch64) grpcurl_arch="arm64"  ;;
    *) echo "unsupported arch for grpcurl: $arch" >&2; exit 1 ;;
  esac
  tmp=$(mktemp -d)
  curl -fsSL -o "${tmp}/grpcurl.tar.gz" \
    "https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_${grpcurl_arch}.tar.gz"
  tar -xzf "${tmp}/grpcurl.tar.gz" -C "${tmp}"
  install -m 0755 "${tmp}/grpcurl" /usr/local/bin/grpcurl
  rm -rf "${tmp}"
else
  echo "==> grpcurl already installed ($(grpcurl --version 2>&1 | head -1))"
fi

############################################################
# 4. Host tunables from RUNBOOK.md
############################################################
echo "==> applying kernel + sysctl tunables"

# 4a. Larger socket receive buffer absorbs block-payload bursts.
cat > /etc/sysctl.d/99-grpc-bench.conf <<'EOF'
# grpc-bench: prevent provider-side "slow client receiver" disconnects
# under 23-program load with --with-blocks. Defaults (~256KB) are too
# small for full-block streams with include_transactions=true.
net.core.rmem_max = 268435456
net.core.rmem_default = 16777216
net.core.wmem_max = 268435456
net.core.netdev_max_backlog = 5000
EOF
sysctl -p /etc/sysctl.d/99-grpc-bench.conf

# 4b. CPU governor — pin to performance. Persists across reboots via
# a systemd unit so the setting survives droplet restarts.
cat > /etc/systemd/system/grpc-bench-governor.service <<'EOF'
[Unit]
Description=grpc-bench: pin cpufreq governor to performance
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/usr/bin/cpupower frequency-set --governor performance
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now grpc-bench-governor.service || \
  echo "    (cpupower may not be supported on this hypervisor; continuing)"

# 4c. Transparent hugepages — madvise (avoids TLB-shootdown spikes
# that show as ms-scale tail latency).
cat > /etc/systemd/system/grpc-bench-thp.service <<'EOF'
[Unit]
Description=grpc-bench: set transparent_hugepage to madvise
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c "echo madvise > /sys/kernel/mm/transparent_hugepage/enabled"
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now grpc-bench-thp.service

############################################################
# 5. Optional: build + grant CAP_SYS_NICE on the binary
############################################################
if [[ $DO_BUILD -eq 1 ]]; then
  echo "==> building grpc-bench (release)"
  REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
  if [[ ! -f "${REPO_DIR}/Cargo.toml" ]]; then
    echo "    Cargo.toml not found at ${REPO_DIR} — skipping build" >&2
  else
    sudo -u "${INVOKING_USER}" -H bash -c "
      source '${INVOKING_HOME}/.cargo/env'
      cd '${REPO_DIR}'
      cargo build --release
    "
    BIN="${REPO_DIR}/target/release/grpc-bench"
    if [[ -x "$BIN" ]]; then
      echo "==> granting CAP_SYS_NICE to ${BIN}"
      setcap cap_sys_nice=eip "$BIN"
      echo "    capabilities: $(getcap "$BIN")"
    fi
  fi
fi

############################################################
# 6. Posture summary — what's active right now
############################################################
echo
echo "==> posture summary"
printf "  kernel:           %s\n" "$(uname -r)"
printf "  governor:         %s\n" "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unavailable)"
printf "  transparent_hugepage: %s\n" "$(awk -F'[][]' '{print $2}' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo unavailable)"
printf "  rmem_max:         %s\n" "$(sysctl -n net.core.rmem_max)"
printf "  chrony tracking:  %s\n" "$(chronyc tracking 2>/dev/null | awk '/Reference ID/{print $4, $5}')"
printf "  grpcurl:          %s\n" "$(grpcurl --version 2>&1 | head -1)"
printf "  rustup:           %s\n" "$(sudo -u "${INVOKING_USER}" -H bash -lc 'rustup --version' 2>/dev/null | head -1 || echo not-installed)"

echo
echo "==> done"
echo
echo "Next steps:"
echo "  1. su - ${INVOKING_USER}     (or open a new shell to pick up cargo PATH)"
echo "  2. cd /path/to/grpc-bench"
if [[ $DO_BUILD -eq 0 ]]; then
  echo "  3. cargo build --release"
  echo "  4. sudo setcap cap_sys_nice=eip target/release/grpc-bench"
fi
echo "  $((DO_BUILD == 1 ? 3 : 5)). Follow RUNBOOK.md — start with Test 1 (60-second posture check)"
