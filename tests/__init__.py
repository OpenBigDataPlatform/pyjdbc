from pyjdbc.java import Jvm

try:
    import faulthandler

    faulthandler.enable()
    faulthandler.disable()
except Exception:
    pass

Jvm.start()