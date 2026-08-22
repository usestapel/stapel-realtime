"""Emit endpoints — a module's call site, reduced to its essentials.

``/emit`` does what ``mutate_and_emit()`` does: writes a row inside
``transaction.atomic()`` and calls ``stapel_core.comm.signal()``, which
schedules delivery on commit through the transport this library registers.
``/emit-rollback`` does the same and then raises, which is the whole point of
the on_commit hook: the signal must not describe a row that never landed.

Note what is NOT imported here: nothing from ``stapel_realtime``. A module
that only emits depends on the core and nothing else — that is the split the
whole design rests on.
"""
import json
import time

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from stapel_core.comm import signal

from .models import Beat


def health(request):
    return JsonResponse({"ok": True})


@csrf_exempt
def emit(request):
    body = json.loads(request.body or b"{}")
    stream = body["stream"]
    count = int(body.get("count", 1))
    signal_type = body.get("type", "beat.tick")

    started = time.monotonic()
    with transaction.atomic():
        for index in range(count):
            beat = Beat.objects.create(label=f"{signal_type}-{index}")
            signal(stream, signal_type, {"beat_id": beat.pk, "index": index})
    return JsonResponse(
        {"emitted": count, "seconds": round(time.monotonic() - started, 4)}
    )


class _Rollback(Exception):
    pass


@csrf_exempt
def emit_rollback(request):
    body = json.loads(request.body or b"{}")
    try:
        with transaction.atomic():
            Beat.objects.create(label="doomed")
            signal(body["stream"], "beat.doomed", {"doomed": True})
            raise _Rollback()
    except _Rollback:
        pass
    return JsonResponse({"rolled_back": True})
