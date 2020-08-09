from typing import Callable, Optional

# https://stackoverflow.com/questions/2262333/is-there-a-built-in-or-more-pythonic-way-to-try-to-parse-a-string-to-an-integer
def ignore_exception(exception=Exception, default_val=None):
  """Returns a decorator that ignores an exception raised by the function it
  decorates.

  Using it as a decorator:

    @ignore_exception(ValueError)
    def my_function():
      pass

  Using it as a function wrapper:

    int_try_parse = ignore_exception(ValueError)(int)
  """
  def decorator(function):
    def wrapper(*args, **kwargs):
      try:
        return function(*args, **kwargs)
      except exception:
        return default_val
    return wrapper
  return decorator

int_try_parse = ignore_exception(ValueError)(int)
float_try_parse: Callable[[str], Optional[float]] = ignore_exception(ValueError)(float)