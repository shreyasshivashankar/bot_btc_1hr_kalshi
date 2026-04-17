"""Execution: OMS, smart order router, IOC escalation ladder, settlement_prob, reconciliation.

Maker-only on entry; exits may cross via IOC (hard rule #1). `submit()` only returns
after broker ack. Broker state is authoritative — reconcile every 60s (hard rule #7).
"""
