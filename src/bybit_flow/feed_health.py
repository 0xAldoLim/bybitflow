"""Persistent transport and per-symbol freshness, independent of probes."""


def stream_counts(streams, symbols, settings, now):
    alive = fresh = 0
    for symbol in set(symbols):
        book = streams.books.get(symbol) if streams else None
        tape = streams.tapes.get(symbol) if streams else None
        book_fresh = bool(book and book.fresh(now, settings.book_stale_ms))
        tape_fresh = bool(
            tape
            and 0 <= now - tape.last_receipt <= settings.trade_stale_ms
            and -2000 <= now - tape.last_event <= settings.trade_stale_ms
        )
        alive += int(book_fresh or tape_fresh)
        fresh += int(book_fresh and tape_fresh)
    return dict(total=len(set(symbols)), alive=alive, fresh=fresh)


def persistent_health(scanner, now):
    from .lifecycle import source_feed

    selected = stream_counts(scanner.streams, scanner.streams.selected, scanner.settings, now)
    by_source = {}
    for signal in scanner.store.active_signals():
        if signal["source"] != "tradingview":
            by_source.setdefault(signal["source"], set()).add(signal["symbol"])
    active = dict(total=0, alive=0, fresh=0)
    for source, symbols in by_source.items():
        _, streams = source_feed(scanner, source)
        counts = stream_counts(streams, symbols, scanner.settings, now)
        for key in active:
            active[key] += counts[key]
    return dict(
        primary_persistent_stream_health=selected,
        active_lifecycle_stream_health=active,
        selected_streams_total=selected["total"],
        selected_streams_fresh=selected["fresh"],
        active_required_streams_total=active["total"],
        active_required_streams_fresh=active["fresh"],
        persistent_connected=bool(selected["alive"] or active["alive"]),
    )
