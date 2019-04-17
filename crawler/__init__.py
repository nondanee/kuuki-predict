from . import capture

def run(connect):
    capture.pull(connect)
    capture.predict(connect)
    # capture.compact(connect)