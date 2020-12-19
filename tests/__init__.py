from pyjdbc.jpypeutil import start_jvm

try:
    import faulthandler

    faulthandler.enable()
    faulthandler.disable()
except Exception:
    pass

start_jvm()