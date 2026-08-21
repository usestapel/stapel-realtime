"""Emit endpoints — a module's call site, reduced to its essentials.

``/emit`` does what ``mutate_and_emit()`` does: writes a row inside
``transaction.atomic()`` and signals on commit. ``/emit-rollback`` does the
same and then raises, which is the whole point of the on_commit hook: the
signal must not describe a row that never landed.
"""
import json
import time

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from stapel_realtime.delivery import signal_on_commit

from .models import Beat


def health(request):
    return JsonResponse({"ok": True})


@csrf_exempt
def emit(request):
    body = json.loads(request.body or b"{}")
    stream = body["stream"]
    count = int(body.get("count", 1))
    signal_type = body.get("type", "beat")

    started = time.monotonic()
    with transaction.atomic():
        for index in range(count):
            beat = Beat.objects.create(label=f"{signal_type}-{index}")
            signal_on_commit(
                stream, signal_type, {"beat_id": beat.pk, "index": index}
            )
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
            signal_on_commit(body["stream"], "beat", {"doomed": True})
            raise _Rollback()
    except _Rollback:
        pass
    return JsonResponse({"rolled_back": True})
