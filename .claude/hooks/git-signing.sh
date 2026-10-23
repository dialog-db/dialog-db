#!/bin/sh
# Signs commits made in a cloud session with a dedicated OpenPGP key, and
# authors them as that key's user ID, so agent commits are traceable to the
# key rather than to a generic identity. A commit-msg hook strips the agent
# attribution trailers, which the key makes redundant.
#
# The key comes from the GIT_SIGNING_KEY environment variable: the base64 of
# an armored secret key without a passphrase, ideally a signing subkey only
# (`gpg --export-secret-subkeys --armor <SUBKEY_ID>! | base64 -w0`). Without
# it this does nothing, so sessions without the secret are unaffected.
set -eu
[ -n "${GIT_SIGNING_KEY:-}" ] || exit 0

key=$(mktemp)
trap 'rm -f "$key"' EXIT
printf %s "$GIT_SIGNING_KEY" | base64 -d >"$key"

fpr=$(gpg --batch --with-colons --import-options show-only --import "$key" |
  awk -F: '$1 == "fpr" { print $10; exit }')
uid=$(gpg --batch --with-colons --import-options show-only --import "$key" |
  awk -F: '$1 == "uid" { print $10; exit }')
gpg --batch --quiet --import "$key" 2>/dev/null

hooks="$HOME/.config/git/session-hooks"
mkdir -p "$hooks"
cat >"$hooks/commit-msg" <<'HOOK'
#!/bin/sh
sed -i -E \
  -e '/^Co-Authored-By:.*(Claude|@anthropic\.com)/Id' \
  -e '/^Claude-Session:/Id' \
  -e '/Generated with \[Claude Code\]/d' \
  -e '/^https:\/\/claude\.ai\/code\/session_/d' \
  "$1"
# Drop the blank lines the removed trailers leave at the end.
sed -i -e :a -e '/^\n*$/{$d;N;ba' -e '}' "$1"
# core.hooksPath hides the repository's own hooks, so run its commit-msg too.
own="$(git rev-parse --git-dir)/hooks/commit-msg"
if [ -x "$own" ]; then exec "$own" "$@"; fi
HOOK
chmod +x "$hooks/commit-msg"

name=${uid% <*}
email=${uid##*<}
email=${email%>}

# GIT_CONFIG_* and GIT_AUTHOR_*/GIT_COMMITTER_* take precedence over every
# gitconfig file, so they hold even if the global config is rewritten later.
# The variables already set in the environment are left alone.
settings="GIT_CONFIG_COUNT=4
GIT_CONFIG_KEY_0=gpg.format
GIT_CONFIG_VALUE_0=openpgp
GIT_CONFIG_KEY_1=user.signingkey
GIT_CONFIG_VALUE_1=$fpr
GIT_CONFIG_KEY_2=commit.gpgsign
GIT_CONFIG_VALUE_2=true
GIT_CONFIG_KEY_3=core.hooksPath
GIT_CONFIG_VALUE_3=$hooks
GIT_AUTHOR_NAME=${GIT_AUTHOR_NAME:-$name}
GIT_AUTHOR_EMAIL=${GIT_AUTHOR_EMAIL:-$email}
GIT_COMMITTER_NAME=${GIT_COMMITTER_NAME:-$name}
GIT_COMMITTER_EMAIL=${GIT_COMMITTER_EMAIL:-$email}"

if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  printf '%s\n' "$settings" | sed "s/^\([A-Z_0-9]*\)=\(.*\)/export \1='\2'/" >>"$CLAUDE_ENV_FILE"
else
  git config --global gpg.format openpgp
  git config --global user.signingkey "$fpr"
  git config --global commit.gpgsign true
  git config --global core.hooksPath "$hooks"
  git config --global user.name "$name"
  git config --global user.email "$email"
fi
