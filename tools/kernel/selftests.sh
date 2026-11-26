#!/bin/sh
# selftests.sh — automated checks for snd-usb-babyface-pro WITHOUT the
# card: the law selftests (fader/master/display/EQ), the PCM completion
# selftest, the PCM session-contract test, the module build, and
# checkpatch.  The hardware regress (regress.sh) is separate — it needs
# the card + a free device.
#
# Usage: sh selftests.sh          (from tools/kernel or the repo root)
set -u
cd "$(dirname "$0")"
FAIL=0
TMP="${TMPDIR:-/tmp}"

echo "== law selftests =="
# Each selftest runs the driver's own law code: extract_laws.py pulls the
# definitions it names out of the driver sources into a header.
# pcm_selftest runs the URB completion handlers and pcm_session_test the
# session contract (hw_params/prepare/trigger/teardown), both over the
# kernel stand-ins of their shim ahead of the extracted code.
for t in fader_selftest master_selftest disp_selftest eq_selftest pcm_selftest \
	 pcm_session_test; do
	LM=""
	[ "$t" = eq_selftest ] && LM="-lm"
	SHIM=""
	[ "$t" = pcm_selftest ] && SHIM="-include pcm_shim.h"
	[ "$t" = pcm_session_test ] && SHIM="-include pcm_session_shim.h"
	# The session test pulls in the whole session path, whose tables and
	# accessors include a few helpers no assertion calls; do not turn
	# "defined but not used" into a failure there (the driver build and
	# checkpatch still apply).
	WERR="-Werror"
	[ "$t" = pcm_session_test ] && WERR="-Werror -Wno-unused-function -Wno-unused-variable"
	if python3 extract_laws.py "$t.c" > "$TMP/$t-laws.h" &&
	   gcc -O2 -Wall $WERR $SHIM -include "$TMP/$t-laws.h" -o "$TMP/$t" "$t.c" $LM &&
	   "$TMP/$t" > "$TMP/$t.out" 2>&1; then
		echo "  PASS  $t"
		grep -E '^ok' "$TMP/$t.out" | tail -1 > /dev/null
	else
		echo "  FAIL  $t"
		tail -5 "$TMP/$t.out"
		FAIL=1
	fi
done

# SELFTESTS_NO_BUILD=1 stops here: what CI runs, where no kernel tree
# matches the running kernel.
if [ -n "${SELFTESTS_NO_BUILD:-}" ]; then
	echo "== RESULT: $([ $FAIL -eq 0 ] && echo ALL OK || echo FAILURES) (build and checkpatch skipped) =="
	exit $FAIL
fi

echo "== module build =="
KSRC=/lib/modules/$(uname -r)/build
if make LLVM=1 -C "$KSRC" M="$PWD" modules > "$TMP/kbuild.out" 2>&1; then
	echo "  PASS  make modules"
else
	echo "  FAIL  make modules"
	tail -10 "$TMP/kbuild.out"
	FAIL=1
fi

echo "== checkpatch =="
KP="$KSRC/scripts/checkpatch.pl"
if [ -x "$KP" ]; then
	for f in babyfacepro.c babyfacepro-ctl.c babyfacepro.h; do
		if $KP --no-tree --strict -f "$f" 2>&1 | grep -qE 'ERROR|WARNING'; then
			echo "  WARN  $f (see checkpatch below)"
			$KP --no-tree --strict -f "$f" 2>&1 | grep -E 'ERROR|WARNING'
			FAIL=1
		else
			echo "  PASS  $f"
		fi
	done
else
	echo "  SKIP  checkpatch (no $KP)"
fi

echo "== RESULT: $([ $FAIL -eq 0 ] && echo ALL OK || echo FAILURES) =="
exit $FAIL
