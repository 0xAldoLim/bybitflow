"""Delivery diagnostics only; receipts and retry policy remain in the notifier."""

DESTINATIONS = ("signals", "monitoring", "operations")
FAILED = {"error", "failed", "rejected", "uncertain", "pending", "rate-limited"}


def record_transport(store, detail, destination, method):
    result = detail | dict(destination=destination, method=method)
    store.put(f"discord_transport:{destination}", result)
    # Retain the existing POST diagnostic for consumers of the signal funnel.
    store.put("discord_terminal_transport" if method == "PATCH" else "discord_transport", result)


def transport_health(store):
    """Read bounded caches, without changing delivery state or scanning history."""
    destinations = {
        name: result for name in DESTINATIONS if (result := store.get(f"discord_transport:{name}", {}))
    }
    post = store.get("discord_transport", {})
    terminal = store.get("discord_terminal_transport", {})
    legacy_terminal_ms = store.get("last_terminal_delivery_ms", 0) or 0
    if not terminal and legacy_terminal_ms:
        terminal = dict(at_ms=legacy_terminal_ms, status="sent", destination="signals", method="PATCH")
    if terminal and "signals" not in destinations:
        destinations["signals"] = terminal

    historical = None
    if post and not post.get("destination"):
        # Older releases cached only POST failures. A later confirmed PATCH is
        # evidence of a subsequent write, not a receipt for that earlier POST.
        if terminal.get("status") == "sent" and terminal.get("at_ms", 0) > post.get("at_ms", 0):
            if post.get("status") in FAILED:
                historical = post
        else:
            destinations["unknown"] = post
    return dict(
        destinations=destinations,
        historical_unconfirmed_attempt=historical,
        history_note=(
            "Transport results describe the last actual request to each destination, not continuous connectivity. "
            "A later successful write does not confirm an earlier uncertain initial card. "
            "Initial-delivery counts and receipts remain unchanged; uncertain initials are not automatically resent."
        ),
    )
