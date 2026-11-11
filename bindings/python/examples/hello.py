"""Sends text from a source to a receiver on this computer, and renders a binding pattern.

Run it with:  python examples/hello.py
"""

import ntxprotocol as ntx

with ntx.Source("Hello Source", bind="127.0.0.1", port=0, advertise=False) as source:
    with ntx.Receiver(f"127.0.0.1:{source.port}", "Hello Receiver") as receiver:
        seq = source.update_text(ntx.PROGRAM, "Hello from Python")

        while event := receiver.wait_event(timeout=10):
            if isinstance(event, ntx.Connected):
                print(f"connected to {event.source.name}")
            elif isinstance(event, ntx.Update) and event.seq == seq:
                print(f"{event.channel}: {event.fields['text']}")
                break

fields = {"reference": "John 3:16", "version": "KJV"}
print(ntx.render(fields, "{reference} · {version}"))
