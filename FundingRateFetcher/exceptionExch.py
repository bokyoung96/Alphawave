import logging
from typing import Any

logger = logging.getLogger(__name__)

SPECIAL_OPTIONS = {
    'bitget': {
        'fetchFundingRates': ['USDC-FUTURES', 'USDT-FUTURES'],
        'loadMarkets': ['USDC-FUTURES', 'USDT-FUTURES'],
        'fetchTickers': ['USDC-FUTURES', 'USDT-FUTURES'],
        'fetchTradingFees': ['USDC-FUTURES', 'USDT-FUTURES']
    }
}


class ExceptionExchange:
    def __init__(self, exchange):
        self._exchange = exchange
        exch_id = getattr(exchange, 'id', exchange.__class__.__name__).lower()
        self._special_options = SPECIAL_OPTIONS.get(exch_id, {})

    def __getattr__(self, name: str) -> Any:
        base_attr = getattr(self._exchange, name)
        if callable(base_attr) and name in self._special_options:
            def wrapped(*args, **kwargs):
                responses = []
                for pt in self._special_options[name]:
                    if 'params' in kwargs and kwargs['params'] is not None:
                        new_params = {**kwargs['params'], 'productType': pt}
                    else:
                        new_params = {'productType': pt}
                    new_kwargs = {**kwargs, 'params': new_params}
                    try:
                        res = base_attr(*args, **new_kwargs)
                        responses.append(res)
                    except Exception as e:
                        logger.error(
                            f"[ExceptionExchange] Error in {name} for productType {pt}: {e}")
                if responses and all(isinstance(r, dict) for r in responses):
                    merged = {}
                    for d in responses:
                        merged.update(d)
                    return merged
                return responses
            return wrapped
        return base_attr
