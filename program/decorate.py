from functools import wraps


def print_and_exit_if_any_error(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        func_name = func.__name__
        print(f"Running {func_name}...")
        try:
            return func(*args, **kwargs)
        except Exception as err:
            # my_exception_handler(e)
            print(f"Error running {func_name}.")
            print(f"Exception: {err}")
            exit(1)

    return func_wrapper
