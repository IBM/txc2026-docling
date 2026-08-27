"""The COS-event trigger: one Code Engine app for the whole class.

Everything except :mod:`docling_trigger.app` is standard library only. That is
deliberate — the parsing, signing and payload-building rules are the part worth
testing, and they are tested from the repo's ``pytest`` run without installing
a web framework (``tests/test_trigger_*.py``).
"""
