from decorator import decorator
from datadog import initialize
from datadog.threadstats import ThreadStats

import logging
log = logging.getLogger('datadog.lambda')


class LambdaStats(object):
    """ Singleton to share a ThreadStats instance between the wrapper and the lambdas """
    _threadStats_instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._threadStats_instance:
            cls._threadStats_instance = ThreadStats()
        return cls._threadStats_instance


@decorator
def datadog_lambda_wrapper(func, api_key=None, app_key=None, *args, **kw):
    """ Wrapper to automatically initialize the client & flush

    Usage for lambdas:

    @datadog_lambda_wrapper()  # Use env variables DATADOG_API_KEY & DATADOG_APP_KEY
    @datadog_lambda_wrapper(api_key=SOME_KEY, app_key=SOME_KEY)  # You can also use hardcoded keys
    def my_lambda_function(event, context):
        ....

    """

    try:
        initialize(api_key, app_key)
        # Start the ThreadStats in manual flush mode
        LambdaStats().start(flush_in_thread=False, flush_in_greenlet=False)
    except Exception:
        log.exception("Exception occured during Datadog lambda decorator initialization")

    result = func(*args, **kw)  # Run the lambda

    try:
        LambdaStats().flush(float('inf'))  # Flush all metrics
    except Exception:
        log.exception("Exception occured during Datadog lambda decorator stats flush")

    return result
