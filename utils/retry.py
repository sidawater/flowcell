
import types
from functools import wraps


class Retry(object):
    """
    重试装饰器
    """

    def __init__(self, func, retry=1, on_retry=None, on_exit=None, trace_with=None, **kwargs):
        """

        :param func: 被装饰方法
        :param retry: 重试次数
        :param on_retry: 重试条件
            类型: Exception, 捕获指定异常时重试
            类型: function(func返回值) --> bool, 指定函数, 入参为被装饰方法 func 的返回结果, 出参为 bool: 为真时重试
            类型: tuple/list, 多种条件聚合(or关系)
        :param on_exit: 中断条件, 与使用 on_retry 一致, 优先级高于 on_retry
        :param trace_with: 回调方法, 所有重试结束且失败时的回调, 入参 func 返回结果或异常msg
        :param kwargs:
        """

        wraps(func)(self)
        if not (isinstance(retry, int) and retry > 0):
            raise ValueError('重试次数必须大于0')
        self._retry = retry

        self._retry_exceptions, self._retry_funcs = self._build_conditions(on_retry)
        self._exit_exceptions, self._exit_funcs = self._build_conditions(on_exit)

        self.trace_func = trace_with

    def __call__(self, *args, **kwargs):
        return self.run_retry(*args, **kwargs)

    def run_retry(self, *args, **kwargs):
        rty = self._retry
        exc, result = None, None
        while rty > 0:
            rty -= 1
            exc, result = self.run_single(*args, **kwargs)
            # print(exc, result, self._should_rty(exc, result), self._should_exit(exc, result))

            if self._should_exit(exc, result):
                break
            if not self._should_rty(exc, result):
                break

        if self.trace_func:
            self.trace_func(exc or result)

        if exc is None:
            return result
        raise exc

    def _should_rty(self, exc, result):
        """
        判断是否需要重试
        :param exc:
        :param result:
        :return:
        """
        if isinstance(exc, self._retry_exceptions):
            return True

        for func in self._retry_funcs:
            if func(result):
                return True
        return False

    def _should_exit(self, exc, result):
        if isinstance(exc, self._exit_exceptions):
            return True

        for func in self._exit_funcs:
            if func(result):
                return True
        return False

    @staticmethod
    def _build_conditions(conditions):
        if conditions is None:
            return (), []

        if isinstance(conditions, type):
            return conditions, []
        elif not isinstance(conditions, type) and hasattr(conditions, '__call__'):
            return (), [conditions]
        elif hasattr(conditions, '__iter__'):
            _exc = [e for e in conditions if isinstance(e, type)]
            _func = [fc for fc in conditions if not isinstance(fc, type) and hasattr(fc, '__call__')]
            return tuple(_exc), _func
        else:
            raise ValueError('conditions Error! should be one or a series of Exception/function')

    def run_single(self, *args, **kwargs):
        try:
            result = self.__wrapped__(*args, **kwargs)
            return None, result
        except Exception as e:
            return e, None

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            return types.MethodType(self, instance)


def retry(retry=1, on_retry=None, on_exit=None, trace_with=None, **kwargs):

    def wrapper(func):
        # nonlocal kwargs
        drc = DcRetry(func, retry=retry, on_retry=on_retry, on_exit=on_exit, trace_with=trace_with, **kwargs)
        return drc

    return wrapper