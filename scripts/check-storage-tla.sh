#!/usr/bin/env bash
set -euo pipefail

readonly tla_version="1.7.4"
readonly tla_sha256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root
readonly tla_work_root="${TLA_WORK_ROOT:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/skein-tla}"
readonly downloaded_jar="$tla_work_root/tla2tools-$tla_version.jar"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

resolve_tla_jar() {
  if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
    printf '%s\n' "$TLA2TOOLS_JAR"
    return
  fi

  mkdir -p "$tla_work_root"
  if [[ ! -f "$downloaded_jar" ]] ||
    [[ "$(sha256_file "$downloaded_jar")" != "$tla_sha256" ]]; then
    local temporary_jar="$downloaded_jar.tmp"
    curl --fail --location --silent --show-error \
      "https://github.com/tlaplus/tlaplus/releases/download/v$tla_version/tla2tools.jar" \
      --output "$temporary_jar"
    if [[ "$(sha256_file "$temporary_jar")" != "$tla_sha256" ]]; then
      printf 'downloaded tla2tools.jar checksum mismatch\n' >&2
      return 1
    fi
    mv "$temporary_jar" "$downloaded_jar"
  fi
  printf '%s\n' "$downloaded_jar"
}

tla_jar="$(resolve_tla_jar)"
readonly tla_jar
readonly tla_java="${TLA_JAVA:-java}"
readonly specifications=(
  SkeinStorageDurability
  SkeinGenerationReclamation
  SkeinConcurrentSnapshots
  SkeinSourceSegmentPublication
)

for specification in "${specifications[@]}"; do
  model_state_dir="$tla_work_root/states/$specification"
  mkdir -p "$model_state_dir"
  "$tla_java" -XX:+UseParallelGC -jar "$tla_jar" \
    -cleanup \
    -metadir "$model_state_dir" \
    -workers auto \
    -config "$repository_root/docs/tla/$specification.cfg" \
    "$repository_root/docs/tla/$specification.tla"
done
